package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * 代表堆文件（HeapFile）中的一页数据
 * <p>
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    // 页面 ID
    final HeapPageId pid;
    // 元组描述符，表示页面存储的数据类型
    final TupleDesc td;
    // 字节数组，表示页面头部文件，用来标记哪些槽位被占用。
    // 每个字节（8 位）表示 8 个槽位（tuples）。每一位表示一个槽位是否被占用（1是0否）。
    final byte[] header;
    // 存储实际元组数据的数组
    final Tuple[] tuples;
    // 页面中可用的槽位数量（即该页面能够存储的元组数量）
    final int numSlots;
    // 用来存储该页面数据的副本，用于实现恢复功能
    byte[] oldData;
    // 用于保护 oldData 的访问，确保线程安全
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * 中文翻译：
     * <p>
     * 从磁盘读取字节数据创建一个 HeapPage。
     * <p>
     * HeapPage 的格式是由一组头部字节组成，指示页面中哪些槽位正在使用，同时包含一定数量的元组槽位。
     * <p>
     * 具体来说，元组的数量等于：floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * 其中， 其中，元组大小是当前数据库表中元组的大小，可以通过 {@link Catalog#getTupleDesc} 获取。
     * <p>
     * 页面头部的字节数为：ceiling(no. tuple slots / 8)
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();

        // 从 data 中读取字节数据
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 读取头部数据
        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        // 读取元组数据
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }

        dis.close();

        // 设置恢复数据的副本
        setBeforeImage();
    }

    /**
     * 获取元组数量
     * <p>
     * 计算当前页面上可以存储多少个元组
     * <p>
     * Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        // 构造函数里的注释：元组的数量 = floor((BufferPool. getPageSize()*8) / (tuple size * 8 + 1))
        return (int) Math.floor((BufferPool.getPageSize() * 8.0) / (td.getSize() * 8 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) Math.ceil(getNumTuples() * 1.0 / 8);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * 读取下一个元组
     * <p>
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * 获取页面数据
     * <p>
     * 将当前页面的数据转换为字节数组，以便序列化到磁盘
     * <p>
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // 写入页面的头部信息
        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // 遍历所有元组，写入每个元组的字段数据
        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot 空槽填充零字节
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 根据页面的剩余空间填充零字节，确保字节数组的大小等于页面大小
        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * 删除元组
     * <p>
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * 插入元组
     * <p>
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * 设置页面是否为脏页（即是否已被修改）
     * <p>
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
    }

    /**
     * 查询页面是否为脏页，返回最后修改该页面的事务 ID。
     * <p>
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int count = 0;
        // 遍历所有的槽位
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 如果无头绪，那就去看 header 上的注释
     * @param i 槽位索引
     * @return Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // 计算字节索引：i / 8
        int byteIndex = i / 8;
        // 计算该字节中位置的位索引：i % 8
        int bitIndex = i % 8;

        // 取出 header 中对应的字节
        int headerByte = header[byteIndex];

        // 使用位操作，检查该字节对应的 bitIndex 是否为 1
        return ((headerByte >> bitIndex) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> usedTuples = new ArrayList<>();
        // 遍历所有槽位
        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i)) {
                usedTuples.add(tuples[i]);
            }
        }
        return usedTuples.iterator();
    }

}

