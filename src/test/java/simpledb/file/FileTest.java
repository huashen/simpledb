package simpledb.file;

import org.junit.Assert;
import org.junit.Test;
import simpledb.server.SimpleDB;

public class FileTest {

    @Test
    public void test01() {
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        FileMgr fm = db.fileMgr();
        BlockId blk = new BlockId("testfile", 2);
        int pos1 = 88;

        Page p1 = new Page(fm.blockSize());
        p1.setString(pos1, "abcdefghijklm");
        int size = Page.maxLength("abcdefghijklm".length());
        int pos2 = pos1 + size;
        p1.setInt(pos2, 345);
        fm.write(blk, p1);

        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);

        Assert.assertEquals(p2.getString(pos1), "abcdefghijklm");
        Assert.assertEquals(p2.getInt(pos2), 345);
    }
}