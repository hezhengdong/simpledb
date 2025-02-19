package simpledb.storage;

import simpledb.common.Type;
import simpledb.common.Utility;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * HeapFileEncoder 读取以逗号分隔的文本文件或接受一个元组数组，并将其转换为适合 simpledb 堆页面格式的二进制数据页 页面填充为指定长度，并连续写入数据文件。
 * <p>
 * HeapFileEncoder reads a comma delimited text file or accepts
 * an array of tuples and converts it to
 * pages of binary data in the appropriate format for simpledb heap pages
 * Pages are padded out to a specified length, and written consecutive in a
 * data file.
 */

public class HeapFileEncoder {

    /**
     * 接收元组列表（只有整数字段），转换为临时文件，再调用 convert 转换为二进制页面文件。
     * <p>
     * 输出文件的格式与 HeapPage 和 HeapFile 中指定的格式相同。
     * <p>
     * Convert the specified tuple list (with only integer fields) into a binary
     * page file. <br>
     * <p>
     * The format of the output file will be as specified in HeapPage and
     * HeapFile.
     *
     * @param tuples     the tuples - a list of tuples, each represented by a list of integers that are
     *                   the field values for that tuple.
     * @param outFile    The output file to write data to
     * @param npagebytes The number of bytes per page in the output file
     * @param numFields  the number of fields in each input tuple
     * @throws IOException if the temporary/output file can't be opened
     * @see HeapPage
     * @see HeapFile
     */
    public static void convert(List<List<Integer>> tuples, File outFile, int npagebytes, int numFields) throws IOException {
        // 创建一个临时文件用于存储输入的元组数据
        File tempInput = File.createTempFile("tempTable", ".txt");
        // 设置临时文件在程序退出时自动删除
        tempInput.deleteOnExit();
        // 创建一个BufferedWriter对象用于向临时文件写入数据
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
        // 遍历每个元组
        for (List<Integer> tuple : tuples) {
            int writtenFields = 0;
            // 遍历元组中的每个字段
            for (Integer field : tuple) {
                writtenFields++;
                // 如果元组中的字段数超过指定的字段数，则抛出异常
                if (writtenFields > numFields) {
                    throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
                            Utility.listToString(tuple) + ")");
                }
                // 将字段值写入临时文件
                bw.write(String.valueOf(field));
                // 如果不是最后一个字段，则写入逗号分隔符
                if (writtenFields < numFields) {
                    bw.write(',');
                }
            }
            // 每写完一个元组，写入换行符
            bw.write('\n');
        }
        // 关闭BufferedWriter对象
        bw.close();
        // 调用另一个convert方法将临时文件转换为二进制页文件
        convert(tempInput, outFile, npagebytes, numFields);
    }

    /**
     * 填充字段类型
     */
    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields) throws IOException {
        // 创建一个Type数组，用于存储每个字段的类型，这里假设所有字段都是整数类型
        Type[] ts = new Type[numFields];
        // 使用Arrays.fill方法将Type数组中的所有元素设置为Type.INT_TYPE
        Arrays.fill(ts, Type.INT_TYPE);
        // 调用另一个convert方法，传入字段类型数组
        convert(inFile, outFile, npagebytes, numFields, ts);
    }

    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields, Type[] typeAr)
            throws IOException {
        // 调用另一个convert方法，传入字段分隔符为逗号
        convert(inFile, outFile, npagebytes, numFields, typeAr, ',');
    }

    /**
     * 核心，负责将输入文件中的每个元组转换为符合 SimpleDB 页面格式的二进制数据。
     * <p>
     * Convert the specified input text file into a binary
     * page file. <br>
     * 将指定的输入文本文件转换为二进制页面文件。<br>
     * Assume format of the input file is (note that only integer fields are
     * supported):<br>
     * 假设输入文件格式为（注意只支持整数字段）：<br>
     * int,...,int\n<br>
     * int,...,int\n<br>
     * ...<br>
     * where each row represents a tuple.<br>
     * <p>
     * The format of the output file will be as specified in HeapPage and
     * HeapFile.
     *
     * @param inFile     The input file to read data from
     * @param outFile    The output file to write data to
     * @param npagebytes The number of bytes per page in the output file
     * @param numFields  the number of fields in each input line/output tuple
     * @throws IOException if the input/output file can't be opened or a
     *                     malformed input line is encountered
     * @see HeapPage
     * @see HeapFile
     */
    public static void convert(File inFile, File outFile, int npagebytes,
                               int numFields, Type[] typeAr, char fieldSeparator)
            throws IOException {

        // 1. 初始化和输入数据处理

        // 1.1 计算每个记录的字节数
        int nrecbytes = 0;
        for (int i = 0; i < numFields; i++) {
            nrecbytes += typeAr[i].getLen();
        }
        // 1.2 计算每页可以存储的记录数
        int nrecords = (npagebytes * 8) / (nrecbytes * 8 + 1);  //floor comes for free

        //  per record, we need one bit; there are nrecords per page, so we need
        // nrecords bits, i.e., ((nrecords/32)+1) integers.
        // 1.3 计算页头的字节数
        int nheaderbytes = (nrecords / 8);
        if (nheaderbytes * 8 < nrecords)
            nheaderbytes++;  //ceiling
        int nheaderbits = nheaderbytes * 8;

        // 2. 打开文件并准备读取数据

        // 2.1 初始化输入和输出流
        // BufferedReader对象，用于高效读取输入文件
        BufferedReader br = new BufferedReader(new FileReader(inFile));
        // FileOutputStream对象，用于写入输出文件
        FileOutputStream os = new FileOutputStream(outFile);

        // 2.2 初始化用于读取字符的缓冲区，并设置计数器
        char[] buf = new char[1024]; // 用于临时存储读取到的字符。

        int curpos = 0;      // 记录当前缓冲区的位置。
        int recordcount = 0; // 记录当前页面中已写入的记录数。
        int npages = 0;      // 记录已写入的页面数。
        int fieldNo = 0;     // 用于标记当前处理的字段。

        // 3. 初始化数据流，用于构造页面头部和页面数据

        // 创建ByteArrayOutputStream对象用于存储页头数据
        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
        // 创建DataOutputStream对象用于写入页头数据
        DataOutputStream headerStream = new DataOutputStream(headerBAOS);
        // 创建ByteArrayOutputStream对象用于存储页体数据
        ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
        // 创建DataOutputStream对象用于写入页体数据
        DataOutputStream pageStream = new DataOutputStream(pageBAOS);

        // 4. 逐字符读取输入数据并处理

        boolean done = false;
        boolean first = true;
        while (!done) {
            // 4.1 逐个读取输入文件的字符，直到文件结束。
            int c = br.read();

            // 4.2 处理换行符和字段

            // 忽略Windows/Notepad特殊行结束符
            if (c == '\r')
                continue;

            // 如果读取到换行符，记录数加1
            if (c == '\n') {
                if (first)
                    continue;
                recordcount++;
                first = true;
            } else
                first = false;

            // 4.3 解析字段并写入页体数据
            // 如果读取到字段分隔符、换行符或回车符，处理当前字段
            if (c == fieldSeparator || c == '\n' || c == '\r') {
                String s = new String(buf, 0, curpos);
                // 如果字段类型为整数类型，将字段值写入页体数据流
                if (typeAr[fieldNo] == Type.INT_TYPE) {
                    try {
                        pageStream.writeInt(Integer.parseInt(s.trim()));
                    } catch (NumberFormatException e) {
                        System.out.println("BAD LINE : " + s);
                    }
                }
                // 如果字段类型为字符串类型，将字段值写入页体数据流
                else if (typeAr[fieldNo] == Type.STRING_TYPE) {
                    s = s.trim();
                    int overflow = Type.STRING_LEN - s.length();
                    if (overflow < 0) {
                        s = s.substring(0, Type.STRING_LEN);
                    }
                    pageStream.writeInt(s.length());
                    pageStream.writeBytes(s);
                    while (overflow-- > 0)
                        pageStream.write((byte) 0);
                }
                // 重置当前字段位置
                curpos = 0;
                // 如果读取到换行符，重置字段编号
                if (c == '\n')
                    fieldNo = 0;
                else
                    fieldNo++;

            } else if (c == -1) {
                // 如果读取到文件末尾，设置done标志为true
                done = true;

            } else {
                // 将读取到的字符存储到字符数组中
                buf[curpos++] = (char) c;
                continue;
            }

            // 5. 写入页头和页体数据

            // 如果当前页已满或读取到文件末尾且当前页有记录，写入页头和页体数据
            if (recordcount >= nrecords
                    || done && recordcount > 0
                    || done && npages == 0) {
                int i = 0;
                byte headerbyte = 0;

                // 写入页头数据
                for (i = 0; i < nheaderbits; i++) {
                    if (i < recordcount)
                        headerbyte |= (1 << (i % 8));

                    if (((i + 1) % 8) == 0) {
                        headerStream.writeByte(headerbyte);
                        headerbyte = 0;
                    }
                }

                if (i % 8 > 0)
                    headerStream.writeByte(headerbyte);

                // 用零填充页体数据的剩余部分
                for (i = 0; i < (npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
                    pageStream.writeByte(0);

                // 将页头和页体数据写入输出文件
                headerStream.flush();
                headerBAOS.writeTo(os);
                pageStream.flush();
                pageBAOS.writeTo(os);

                // 重置页头和页体数据流，准备写入下一页数据
                headerBAOS = new ByteArrayOutputStream(nheaderbytes);
                headerStream = new DataOutputStream(headerBAOS);
                pageBAOS = new ByteArrayOutputStream(npagebytes);
                pageStream = new DataOutputStream(pageBAOS);

                recordcount = 0;
                npages++;
            }
        }

        // 7. 关闭BufferedReader和FileOutputStream对象
        br.close();
        os.close();
    }
}
