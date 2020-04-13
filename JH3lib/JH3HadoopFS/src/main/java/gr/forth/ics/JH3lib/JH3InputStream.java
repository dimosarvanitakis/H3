package gr.forth.ics.JH3lib;

import java.io.EOFException;
import java.io.IOException;

import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.log4j.Logger;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JH3InputStream extends FSInputStream implements CanSetReadahead {

	private final Logger log = Logger.getLogger(JH3InputStream.class);

	/* Position that is set by seek() and returned by getPos() */
	private long pos;

	/*
	 * Closed bit. Volatile so reads are non-blocking. Updates must be in a
	 * synchronized block to guarantee an atomic check and set
	 */
	private volatile boolean closed;
	private final JH3 client;
	private final String bucket;
	private final String key;
	private final String uri;

	/**
	 * 
	 */
	private final long contentLength;

	/**
	 * The start of the content range of the last request. This is an absolute value
	 * of the range, not a length field.
	 */
	private long contentRangeStart;

	/**
	 * How many bytes were read ahead of the stream.
	 */
	private long readahead;
	/*
	 * The end of the content range of the last request. This is an absolute value
	 * of the range, not a length field.
	 */
	private long contentRangeFinish;

	/*
	 * This is the actual position within the object, used by lazy seek to decide
	 * whether to seek on the next read or not.
	 */
	// private long nextReadPos;

	/**
	 * Current chunk of data that has been read
	 */
	private JH3Object dataChunk;

	public JH3InputStream(JH3 client, String bucket, String key, long contentLength, long readahead)
			throws IOException {
		log.trace("JH3InputSteam - " + "client: " + client + ", bucket: " + bucket + ", key: " + key
				+ ", contentLength: " + contentLength + ", readahead: " + readahead);

		// Both bucket and key must be non empty
		Preconditions.checkArgument(!("".equals(bucket)), "Empty bucket");
		Preconditions.checkArgument(!("".equals(key)), "Empty key");
		this.bucket = bucket;
		this.key = key;
		this.contentLength = contentLength;
		this.client = client;
		this.uri = "h3://" + this.bucket + "/" + this.key;
		this.readahead = readahead;
		setReadahead(readahead);
		try {
			dataChunk = client.readObject(bucket, key, 0, readahead);
			log.debug("(Constructor) reading object. uri= " + uri + ", offset= 0, readahead= " + readahead + ", status= "
					+ client.getStatus());
			if (dataChunk == null)
				throw new IOException("Couldnt read file at: " + uri);

			// Set ranges of first request
			contentRangeStart = 0;
			contentRangeFinish = dataChunk.getSize();
			log.debug("initialized contentRanges:[" + contentRangeStart +", " + contentRangeFinish +"]");
		} catch (JH3Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public synchronized void seek(long targetPos) throws IOException {
		log.trace("JH3InputStream:seek - targetPos = " + targetPos);

		checkNotClosed();

		// Do not allow negative seek
		if (targetPos < 0) {
			throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
		}

		if (this.contentLength <= 0) {
			return;
		}

		// Lazy seek
		// nextReadPos = targetPos;
		pos = targetPos;
	}

	@Override
	public synchronized long getPos() throws IOException {
		log.trace("JH3InputStream:getPos");
		return pos;
		// return (nextReadPos < 0) ? 0 : nextReadPos;
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		log.trace("JH3InputStream:seekToNewSource - " + "targetPos: " + targetPos);
		return false;
	}

	@Override
	public synchronized int read() throws IOException {
		log.trace("JH3InputStream:read");

		checkNotClosed();

		// EOF condition
		if (this.contentLength == 0 || (getPos() >= contentLength))
			return -1;

		try {
			if (pos >= contentRangeFinish) {
				// Need to read next chunk into buffer
				log.debug("read - Reading next chunk of data");

				dataChunk = client.readObject(bucket, key, pos, readahead);
				if (dataChunk == null)
					throw new IOException("Couldn't read file at: " + uri);

				log.debug("read - readObject. uri= " + uri + ", offset= " + pos + ", readahead= " + readahead
						+ ", status= " + client.getStatus());

				log.debug("read - contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
				// update request ranges
				contentRangeStart = pos;
				contentRangeFinish = pos + dataChunk.getSize();
				log.debug("read - updated contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
			} else if (pos < contentRangeStart) {
				// Backwards seek
				log.debug("read - Reading previous chunk of data");
				dataChunk = client.readObject(bucket, key, pos, readahead);
				if (dataChunk == null)
					throw new IOException("Couldn't read file at: " + uri);

				log.debug("read -readObject: uri= " + uri + ", offset= " + pos + ", readahead= " + readahead + ", status= "
						+ client.getStatus());
				
				log.debug("read - contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
				// update request ranges
				contentRangeStart = pos;
				contentRangeFinish = pos + dataChunk.getSize();
				log.debug("read - updated contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
			}

			// Get index based on offsets
			int positionedIndex = contentRangeStart == 0 ? 0 : (int) (pos % contentRangeStart);
			int b = dataChunk.getData()[positionedIndex];
			pos++;
			log.debug("read - updated offset: " + pos);
			return b;
		} catch (JH3Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public synchronized int read(byte[] buffer, int offset, int length) throws IOException {
		log.trace("JH3InputStream:read - buffer = " + buffer + ", offset = " + offset + ", length = " + length);

		checkNotClosed();
		// validatePositionedReadArgs(pos, buffer, offset, length);

		if (length == 0)
			return 0;

		if (this.contentLength == 0 || (getPos() >= this.contentLength))
			return -1;

		// Calculate how many bytes to read
		int remaining = this.available();
		int bytesToRead = Math.min(remaining, length);
		try {
			if (pos >= contentRangeFinish) {
				// Need to read next chunk into buffer
				log.debug("readMany - Reading next chunk of data");

				dataChunk = client.readObject(bucket, key, pos, readahead);
				if (dataChunk == null)
					throw new IOException("Couldn't read file at: " + uri);

				log.debug("readMany - readObject. uri= " + uri + ", offset= " + pos + ", readahead= " + readahead
						+ ", status= " + client.getStatus());

				log.debug("readMany - contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
				// update request ranges
				contentRangeStart = pos;
				contentRangeFinish = pos + dataChunk.getSize();
				log.debug("readMany - updated contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");

			} else if (pos < contentRangeStart) {
				// Backwards seek
				log.debug("readMany - Reading previous chunk of data");
				dataChunk = client.readObject(bucket, key, pos, readahead);
				if (dataChunk == null)
					throw new IOException("Couldn't read file at: " + uri);

				log.debug("readMany - readObject: uri= " + uri + ", offset= " + pos + ", readahead= " + readahead
						+ ", status= " + client.getStatus());

				log.debug("readMany - contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
				// update request ranges
				contentRangeStart = pos;
				contentRangeFinish = pos + dataChunk.getSize();
				log.debug("readMany - updated contentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
			}

			// Get index based on offsets
			int positionedIndex = contentRangeStart == 0 ? 0 : (int) (pos % contentRangeStart);
			int nread = Math.min(bytesToRead, (int) dataChunk.getSize() - positionedIndex);
			log.debug("readMany - copying from offset: " + positionedIndex + " #" + nread + " bytes");
			log.debug("readMany - position: " + pos + " ContentRanges: [" + contentRangeStart + ", " + contentRangeFinish + "]");
				System.arraycopy(dataChunk.getData(), positionedIndex, buffer, 0, nread);

			while (nread < bytesToRead) {
				dataChunk = client.readObject(bucket, key, contentRangeFinish, readahead);

				if (dataChunk == null)
					throw new IOException("Couldn't read file at: " + uri);

				log.debug("readObject: uri= " + uri + ", offset= " + contentRangeFinish + ", readahead= " + readahead
						+ ", status= " + client.getStatus());

				contentRangeStart = contentRangeFinish;
				contentRangeFinish = contentRangeFinish + readahead;
				pos = contentRangeStart;
				int nbytes = Math.min(bytesToRead - nread, (int) dataChunk.getSize());
				log.debug("readMany - copying from offset: 0" + "to offset: " + nread + " #" + nbytes + " bytes");
				System.arraycopy(dataChunk.getData(), 0, buffer, nread, nbytes);
				nread += nbytes;
			}

			// Move position depending on how many bytes were read
			log.debug("readMany - offset: " + pos);
			pos += bytesToRead;
			log.debug("readMany - updated offset: " + pos);
			log.debug("readMany - bytesRead: " + bytesToRead);

			return bytesToRead;
		} catch (JH3Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * Operation which only seeks at the start of the series of operations; seeking
	 * back at the end
	 */
	@Override
	public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
		log.trace("JH3InputStream:readFully - " + "position: " + position + ", buffer: " + buffer + ", offset: "
				+ offset + ", length: " + length);

		checkNotClosed();
		// validatePositionedReadArgs(position, buffer, offset, length);
		int nread = 0;
		synchronized (this) {
			long oldPos = getPos();
			try {
				seek(position);
				while (nread < length) {
					int nbytes = read(buffer, offset + nread, length - nread);
					if (nbytes < 0) {
						throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
					}
					nread += nbytes;
				}
			} finally {
				seekQuietly(oldPos);
			}
		}
	}

	@Override
	public synchronized int available() throws IOException {
		log.trace("JH3InputStream:available");

		long remaining = remainingInFile();

		if (remaining > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;

		return (int) remaining;
	}

	@Override
	public synchronized void close() throws IOException {
		log.trace("JH3InputStream:close");

		if (!closed) {
			closed = true;
			try {
				super.close();
			} finally {
				log.debug("Ignoring IOE on superclass close(). uri= " + uri);
			}
		}
	}

	@Override
	public synchronized void setReadahead(Long readahead) {
		log.trace("JH3InputStream:setReadahead - " + "readahead: " + readahead);

		if (readahead == null) {
			/* Default value */
			this.readahead = 64 * 1024;
		} else {
			Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
			this.readahead = readahead;
		}
	}

	/* How many bytes are left to read */
	public synchronized long remainingInFile() {
		log.trace("JH3InputStream:remainingInFile: " + (this.contentLength - this.pos));
		return this.contentLength - this.pos;
	}

	public synchronized long getReadahead() {
		log.trace("JH3InputStream:getReadahead");
		return readahead;
	}

	/**
	 * H3 Input stream does not support the mark and reset methods.
	 */
	@Override
	public boolean markSupported() {
		log.trace("JH3InputStream:markSupported");
		return false;
	}

	// private void lazySeek(long targetPos, long len) throws IOException {}

	private void checkNotClosed() throws IOException {
		log.trace("JH3InputStream:checkNotClosed");

		if (closed) {
			throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
		}
	}

	/* Seek without raising any exception. Used in finally clauses */
	private void seekQuietly(long positiveTargetPos) {
		try {
			seek(positiveTargetPos);
		} catch (IOException ioe) {
			log.debug("Ignoring IOE on seek of " + uri + " to " + positiveTargetPos);
		}
	}
}
