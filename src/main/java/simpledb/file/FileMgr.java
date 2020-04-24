package simpledb.file;

import java.io.*;
import java.util.*;

/**
 * 文件管理器
 */
public class FileMgr {
    private File dbDirectory;
    private int blockSize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        // create the directory if the database is new
        //如果是新数据库创建一个指定目录
        if (isNew) {
            dbDirectory.mkdirs();
        }

        // remove any leftover temporary tables
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot read block " + blk);
        }
    }

    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }

    public synchronized BlockId append(String filename) {
        int newBlkNum = length(filename);
        BlockId blk = new BlockId(filename, newBlkNum);
        byte[] b = new byte[blockSize];
        try {
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int) (f.length() / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }


    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        //文件不存在则新建
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }
}
