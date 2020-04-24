package simpledb.log;

import java.util.Iterator;
import simpledb.file.*;

/**
 * The log manager, which is responsible for 
 * writing log records into a log file. The tail of 
 * the log is kept in a bytebuffer, which is flushed
 * to disk when needed. 
 * @author Edward Sciore
 */
public class LogMgr {
   private FileMgr fm;
   private String logfile;
   private Page logPage;
   private BlockId currentBlk;
   private int latestLSN = 0;
   private int lastSavedLSN = 0;

   /**
    * Creates the manager for the specified log file.
    * If the log file does not yet exist, it is created
    * with an empty first block.
    * @param fm the file manager
    * @param logfile the name of the log file
    */
   public LogMgr(FileMgr fm, String logfile) {
      this.fm = fm;
      this.logfile = logfile;
      byte[] b = new byte[fm.blockSize()];
      logPage = new Page(b);
      int logSize = fm.length(logfile);
      if (logSize == 0) {
         currentBlk = appendNewBlock();
      } else {
         currentBlk = new BlockId(logfile, logSize-1);
         fm.read(currentBlk, logPage);
      }
   }

   /**
    * Ensures that the log record corresponding to the
    * specified LSN has been written to disk.
    * All earlier log records will also be written to disk.
    * @param lsn the LSN of a log record
    */
   public void flush(int lsn) {
      if (lsn >= lastSavedLSN) {
         flush();
      }
   }

   public Iterator<byte[]> iterator() {
      flush();
      return new LogIterator(fm, currentBlk);
   }

   /**
    * Appends a log record to the log buffer. 
    * The record consists of an arbitrary array of bytes. 
    * Log records are written right to left in the buffer.
    * The size of the record is written before the bytes.
    * The beginning of the buffer contains the location
    * of the last-written record (the "boundary").
    * Storing the records backwards makes it easy to read
    * them in reverse order.
    * @param logrec a byte buffer containing the bytes.
    * @return the LSN of the final value
    */
   public synchronized int append(byte[] logrec) {
      int boundary = logPage.getInt(0);
      int recsize = logrec.length;
      int bytesneeded = recsize + Integer.BYTES;
      if (boundary - bytesneeded < Integer.BYTES) { // the log record doesn't fit,
         flush();        // so move to the next block.
         currentBlk = appendNewBlock();
         boundary = logPage.getInt(0);
      }
      int recpos = boundary - bytesneeded;

      logPage.setBytes(recpos, logrec);
      logPage.setInt(0, recpos); // the new boundary
      latestLSN += 1;
      return latestLSN;
   }

   /**
    * Initialize the bytebuffer and append it to the log file.
    */
   private BlockId appendNewBlock() {
      BlockId blk = fm.append(logfile);     
      logPage.setInt(0, fm.blockSize());
      fm.write(blk, logPage);
      return blk;
   }

   /**
    * Write the buffer to the log file.
    */
   private void flush() {
      fm.write(currentBlk, logPage);
      lastSavedLSN = latestLSN;
   }
}
