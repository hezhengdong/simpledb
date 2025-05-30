package simpledb.index;

import java.io.*;
import java.util.*;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 * @author Becca Taft
 */
public class BTreeFile implements DbFile {

	private final File f;
	private final TupleDesc td;
	private final int tableid ;
	private final int keyField;

	/**
	 * Constructs a B+ tree file backed by the specified file.
	 *
	 * @param f - the file that stores the on-disk backing store for this B+ tree
	 *            file.
	 * @param key - the field which index is keyed on
	 * @param td - the tuple descriptor of tuples in the file
	 */
	public BTreeFile(File f, int key, TupleDesc td) {
		this.f = f;
		this.tableid = f.getAbsoluteFile().hashCode();
		this.keyField = key;
		this.td = td;
	}

	/**
	 * Returns the File backing this BTreeFile on disk.
	 */
	public File getFile() {
		return f;
	}

	/**
	 * Returns an ID uniquely identifying this BTreeFile. Implementation note:
	 * you will need to generate this tableid somewhere and ensure that each
	 * BTreeFile has a "unique id," and that you always return the same value for
	 * a particular BTreeFile. We suggest hashing the absolute file name of the
	 * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this BTreeFile.
	 */
	public int getId() {
		return tableid;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	/**
	 * Read a page from the file on disk. This should not be called directly
	 * but should be called from the BufferPool via getPage()
	 *
	 * @param pid - the id of the page to read from disk
	 * @return the page constructed from the contents on disk
	 */
	public Page readPage(PageId pid) {
		BTreePageId id = (BTreePageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            if (id.pgcateg() == BTreePageId.ROOT_PTR) {
                byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                return new BTreeRootPtrPage(id, pageBuf);
            } else {
                byte[] pageBuf = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) !=
                        BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException(
                            "Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                if (id.pgcateg() == BTreePageId.INTERNAL) {
                    return new BTreeInternalPage(id, pageBuf, keyField);
                } else if (id.pgcateg() == BTreePageId.LEAF) {
                    return new BTreeLeafPage(id, pageBuf, keyField);
                } else { // id.pgcateg() == BTreePageId.HEADER
                    return new BTreeHeaderPage(id, pageBuf);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

	/**
	 * Write a page to disk.  This should not be called directly but should
	 * be called from the BufferPool when pages are flushed to disk
	 *
	 * @param page - the page to write to disk
	 */
	public void writePage(Page page) throws IOException {
		BTreePageId id = (BTreePageId) page.getId();

		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		if(id.pgcateg() == BTreePageId.ROOT_PTR) {
			rf.write(data);
			rf.close();
		}
		else {
			rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
			rf.write(data);
			rf.close();
		}
	}

	/**
	 * Returns the number of pages in this BTreeFile.
	 */
	public int numPages() {
		// we only ever write full pages
		return (int) ((f.length() - BTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
	}

	/**
	 * Returns the index of the field that this B+ tree is keyed on
	 */
	public int keyField() {
		return keyField;
	}

	/**
	 * 递归函数，找到并锁定与给定关键字段 f 对应的 B+ 树中的叶子页面。<br>
	 * 它会锁定从根节点到叶子节点路径上的所有内部节点，并且给叶子节点加上指定权限的锁。<br>
	 *
	 * 如果 f 为 null，则会找到最左边的叶子页面 —— 用于迭代器。<br>
	 *
	 * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
	 * the left-most page possibly containing the key field f. It locks all internal
	 * nodes along the path to the leaf node with READ_ONLY permission, and locks the
	 * leaf node with permission perm.<br>
	 *
	 * If f is null, it finds the left-most leaf page -- used for the iterator<br>
	 *
	 * @param tid - the transaction id<br>事务 ID
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages<br>脏页面列表，所有新脏页面将会更新到这个列表中
	 * @param pid - the current page being searched<br>当前正在搜索的页面
	 * @param perm - the permissions with which to lock the leaf page<br>给叶子页面设置的锁权限
	 * @param f - the field to search for<br>要搜索的字段
	 * @return the left-most leaf page possibly containing the key field f<br>返回可能包含关键字段 f 的最左边叶子页面
	 *
	 */
	private BTreeLeafPage findLeafPage(TransactionId tid,
									   Map<PageId, Page> dirtypages,
									   BTreePageId pid,
									   Permissions perm,
									   Field f)
			throws DbException, TransactionAbortedException {
		// some code goes here
		// 如果当前页面类型是叶子页面，则直接返回该叶子页面
		if (pid.pgcateg() == BTreePageId.LEAF){
			return (BTreeLeafPage) getPage(tid, dirtypages, pid, perm);  // 获取并锁定叶子页面
		}

		// 如果当前页面是内部页面，获取该页面（只读权限）
		BTreeInternalPage internalPage = (BTreeInternalPage) getPage(tid, dirtypages, pid, Permissions.READ_ONLY);

		// 获取内部页面的条目迭代器
		Iterator<BTreeEntry> it = internalPage.iterator();
		BTreeEntry entry = null;

		// 遍历内部页面的条目
		while (it.hasNext()){
			entry = it.next();
			// 如果未指定索引字段（f==null），默认查找左子树
			if (f == null){
				return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);  // 递归查找左子树的叶子页面
			}
			// 如果当前条目键值 >= f，则查找当前条目的左子树
			if (entry.getKey().compare(Op.GREATER_THAN_OR_EQ, f)){
				return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);  // 递归查找左子树
			}
		}
        // 如果f大于所有条目键值，则查找内部页中最后一个条目的右子树
		return findLeafPage(tid, dirtypages, entry.getRightChild(), perm, f);  // 递归查找右子树
	}

	/**
	 * Convenience method to find a leaf page when there is no dirtypages HashMap.
	 * Used by the BTreeFile iterator.
	 * @see #findLeafPage(TransactionId, Map, BTreePageId, Permissions, Field)
	 *
	 * @param tid - the transaction id
	 * @param pid - the current page being searched
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 *
	 */
	BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid,
                               Field f)
					throws DbException, TransactionAbortedException {
		return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
	}

	/**
	 * 分割叶子页面以为新元组腾出空间，并根据需要递归地分割父节点以容纳新条目。新条目应该具有与右侧页面中第一个元组的键字段相匹配的键（该键会'向上复制'），并且子指针指向分割后生成的两个叶子页面。根据需要更新兄弟指针和父指针。<br>
	 *
	 * Split a leaf page to make room for new tuples and recursively split the parent node
	 * as needed to accommodate a new entry. The new entry should have a key matching the key field
	 * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
	 * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
	 * pointers as needed.  <br>
	 *
	 * 返回将插入具有给定键字段 'field' 的新元组的叶子页面。<br>
	 *
	 * Return the leaf page into which a new tuple with key field "field" should be inserted.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page to split
	 * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
	 *
	 * @return the leaf page into which the new tuple should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        //
        // Split the leaf page by adding a new page on the right of the existing
		// page and moving half of the tuples to the new page.  Copy the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the sibling pointers of all the affected leaf pages.  Return the page into which a
		// tuple with the given key field should be inserted.
		//
		// 分割叶子页面：在现有页面的右侧添加一个新页面，并将一半的元组移动到新页面
		// 将中间键复制到父页面，并根据需要递归分割父页面以容纳新条目。getParentWithEmtpySlots() 在这里会有用。
		// 不要忘记更新所有受影响的叶子页面的兄弟指针。返回将插入给定键字段元组的页面。

		// 先处理子节点：
		// 创建一个右叶节点，并进行均匀分配
		BTreeLeafPage newPage = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);  // 用 getEmptyPage 获得一个新 page
		int tuples = page.getNumTuples(); // 获取当前页面的元组数量

		// 将当前页面的一半元组移动到新的叶子页面
		for (int i = tuples / 2; i < tuples; i++) { // 将一半的 tuple 移到新的 leafPage
			Tuple tupleMove = page.getTuple(i); // 获取当前要移动的元组
			page.deleteTuple(tupleMove); // 从当前页面删除该元组
			newPage.insertTuple(tupleMove); // 将元组插入到新页面
		}

		// 调整左右节点的指针：原来是 leftPage <=> rightPage
		// 现在需要变成 leftPage <=> newPage <=> rightPage
		// 如果当前页有右兄弟页面
		if (page.getRightSiblingId() != null) {
			BTreeLeafPage rightPage = (BTreeLeafPage) getPage(tid, dirtypages, page.getRightSiblingId(), Permissions.READ_WRITE); // 获取右兄弟页面
			newPage.setRightSiblingId(rightPage.getId()); // 设置新页面的右兄弟页面为原右兄弟页面
			rightPage.setLeftSiblingId(newPage.getId()); // 设置右兄弟页面的左兄弟页面为新页面（修改了rightPage，将其加入脏页的跟踪map）
			dirtypages.put(rightPage.getId(), rightPage); // 更新脏页，包含修改后的右兄弟页面
		}
		page.setRightSiblingId(newPage.getId()); // 设置当前页面的右兄弟页面为新页面
		newPage.setLeftSiblingId(page.getId()); // 设置新页面的左兄弟页面为当前页面

		// 开始处理父节点：
		// 将中间的元组"复制"到父节点，并设置父子指针
		Tuple push = newPage.getTuple(0); // 获得要推送给父节点的元组（一般是中间的元组）
		Field key = push.getField(newPage.keyField); // 获取中间元组的键值字段
		BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), field); // 获取父节点页面，并为其腾出空位

		parentPage.insertEntry(new BTreeEntry(key, page.getId(), newPage.getId())); // 将新的条目插入父节点

		// 更新两个叶页的父节点指针
		updateParentPointer(tid, dirtypages, parentPage.getId(), page.getId());
		updateParentPointer(tid, dirtypages, parentPage.getId(), newPage.getId());

		// 将修改后的父节点和当前页面、新页面加入脏页跟踪
		dirtypages.put(parentPage.getId(), parentPage);
		dirtypages.put(page.getId(), page);
		dirtypages.put(newPage.getId(), newPage);

		// 根据字段与父节点的键值进行比较，决定插入哪个页面
		if (field.compare(Op.GREATER_THAN_OR_EQ, key)) { // 如果字段值大于等于父节点的键，则插入新页面
			return newPage;
		} else { // 否则插入原页面
			return page;
		}
	}

	/**
	 * 分割内部页面以为新条目腾出空间，并根据需要递归地分割其父页面以容纳新条目。父条目的新条目应该具有与原始内部页面中间键匹配的键（该键会'推送到父节点'）。新父条目的子指针应该指向分割后生成的两个内部页面。根据需要更新父指针。<br>
	 *
	 * Split an internal page to make room for new entries and recursively split its parent page
	 * as needed to accommodate a new entry. The new entry for the parent should have a key matching
	 * the middle key in the original internal page being split (this key is "pushed up" to the parent).
	 * The child pointers of the new parent entry should point to the two internal pages resulting
	 * from the split. Update parent pointers as needed.<br>
	 *
	 * 返回将插入具有给定键字段 'field' 的新条目的内部页面。<br>
	 * Return the internal page into which an entry with key field "field" should be inserted
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page to split
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreeInternalPage page, Field field)
					throws DbException, IOException, TransactionAbortedException {
		// some code goes here
        //
        // Split the internal page by adding a new page on the right of the existing
		// page and moving half of the entries to the new page.  Push the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
		// the parent pointers of all the children moving to the new page.  updateParentPointers()
		// will be useful here.  Return the page into which an entry with the given key field
		// should be inserted.
		//
		// 分割内部页面：在现有页面右侧添加一个新页面，并将一半的条目移动到新页面
		// 将中间键推送到父页面，并根据需要递归分割父节点以容纳新条目。
		// getParentWithEmtpySlots() 会在这里非常有用。不要忘记更新所有转移到新页面的子节点的父指针。
		// updateParentPointers() 也会在这里很有用。返回将插入具有给定键字段的条目的页面。

		// 创建一个新的内部页面，并准备分割
		BTreeInternalPage newPage = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);  // 用getEmptyPage获得一个新的内部页面
		int numEntries = page.getNumEntries();  // 获取当前内部页面的条目数

		// 转移元组
		Iterator<BTreeEntry> iterator = page.iterator();
		for (int i = 0; i < numEntries / 2; i++) {
			iterator.next();
		}
		BTreeEntry push = iterator.next(); // 初始化条目
		page.deleteKeyAndRightChild(push);
		push.setLeftChild(page.getId());
		push.setRightChild(newPage.getId());
		for (int i = numEntries / 2 + 1; i < numEntries; i++) {
			BTreeEntry curEntry = iterator.next();
			page.deleteKeyAndRightChild(curEntry);
			newPage.insertEntry(curEntry);
		}

		// 获取父页面，并插入条目
		BTreeInternalPage parentPage = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), push.getKey());
		parentPage.insertEntry(push);

		// 指针更新
		updateParentPointers(tid, dirtypages, page);  // 更新当前页面的指针
		updateParentPointers(tid, dirtypages, newPage);  // 更新新页面的指针
		updateParentPointers(tid, dirtypages, parentPage);  // 更新父页面的指针

		// 将修改后的页面加入脏页跟踪
		dirtypages.put(parentPage.getId(), parentPage);  // 更新脏页，包含父页面
		dirtypages.put(page.getId(), page);  // 更新脏页，包含当前页面
		dirtypages.put(newPage.getId(), newPage);  // 更新脏页，包含新页面

		// 比较字段和父节点的键值，决定将哪个页面返回
		if (field.compare(Op.GREATER_THAN_OR_EQ, push.getKey())) {  // 如果字段值大于或等于父节点的键值
			return newPage;  // 返回新页面
		} else {  // 否则返回当前页面
			return page;
		}
	}

	/**
	 * Method to encapsulate the process of getting a parent page ready to accept new entries.
	 * This may mean creating a page to become the new root of the tree, splitting the existing
	 * parent page if there are no empty slots, or simply locking and returning the existing parent page.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param parentId - the id of the parent. May be an internal page or the RootPtr page
	 * @param field - the key of the entry which will be inserted. Needed in case the parent must be split
	 * to accommodate the new entry
	 * @return the parent page, guaranteed to have at least one empty slot
	 * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

		BTreeInternalPage parent = null;

		// create a parent node if necessary
		// this will be the new root of the tree
		if(parentId.pgcateg() == BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

			// update the root pointer
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
					BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
			rootPtr.setRootId(parent.getId());

			// update the previous root to now point to this new root.
			BTreePage prevRootPage = (BTreePage)getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
			prevRootPage.setParentId(parent.getId());
		}
		else {
			// lock the parent page
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId,
					Permissions.READ_WRITE);
		}

		// split the parent if needed
		if(parent.getNumEmptySlots() == 0) {
			parent = splitInternalPage(tid, dirtypages, parent, field);
		}

		return parent;

	}

	/**
	 * Helper function to update the parent pointer of a node.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - id of the parent node
	 * @param child - id of the child node to be updated with the parent pointer
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
			throws DbException, TransactionAbortedException {

		BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

		if(!p.getParentId().equals(pid)) {
			p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
			p.setParentId(pid);
		}

	}

	/**
	 * Update the parent pointer of every child of the given page so that it correctly points to
	 * the parent
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the parent page
	 * @see #updateParentPointer(TransactionId, Map, BTreePageId, BTreePageId)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page)
			throws DbException, TransactionAbortedException{
		Iterator<BTreeEntry> it = page.iterator();
		BTreePageId pid = page.getId();
		BTreeEntry e = null;
		while(it.hasNext()) {
			e = it.next();
			updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
		}
		if(e != null) {
			updateParentPointer(tid, dirtypages, pid, e.getRightChild());
		}
	}

	/**
	 * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
	 * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
	 * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
	 * presumably they will soon be dirtied by this transaction.
	 *
	 * This method is needed to ensure that page updates are not lost if the same pages are
	 * accessed multiple times.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {
		if(dirtypages.containsKey(pid)) {
			return dirtypages.get(pid);
		}
		else {
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if(perm == Permissions.READ_WRITE) {
				dirtypages.put(pid, p);
			}
			return p;
		}
	}

	/**
	 * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
	 * May cause pages to split if the page where tuple t belongs is full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, Map, BTreeLeafPage, Field)
	 */
	public List<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// 存储被修改过的页面
		Map<PageId, Page> dirtypages = new HashMap<>();

		// get a read lock on the root pointer page and use it to locate the root page
		// 获取根指针页面的读锁，并通过根指针页面定位根页面
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        // 获取根页面的 ID
		BTreePageId rootId = rootPtr.getRootId();

		// 如果根页面为空，表示根页面刚刚创建，需要将BTreePage的根指针指向新创建的根页面
		if (rootId == null) { // the root has just been created, so set the root pointer to point to it
			// 创建一个新的叶子页面的 ID，根页面是叶子页面
			rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
			// 获取根指针页面并获得对其的读写权限
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
			// 设置根指针指向新的根页面
			rootPtr.setRootId(rootId);
		}

		// find and lock the left-most leaf page corresponding to the key field,
		// and split the leaf page if there are no more slots available
		// 根据元组的键值查找对应的叶子页面，并且获取该页面的锁
		BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));

		// 如果叶子页面已满（没有空位），则进行叶子页面分裂
		if(leafPage.getNumEmptySlots() == 0) {
			// 分裂叶子页面，返回新的叶子页面
			leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
		}

		// insert the tuple into the leaf page
		// 将元组插入到找到的叶子页面中
		leafPage.insertTuple(t);

        // 返回所有被修改过的页面
		return new ArrayList<>(dirtypages.values());
	}

	/**
	 * Handle the case when a B+ tree page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the page which is less than half full
	 * @see #handleMinOccupancyLeafPage(TransactionId, Map, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * @see #handleMinOccupancyInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page)
			throws DbException, IOException, TransactionAbortedException {
		BTreePageId parentId = page.getParentId();
		BTreeEntry leftEntry = null;
		BTreeEntry rightEntry = null;
		BTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have
		// the same parent as the page. Find the entries in the parent corresponding to
		// the page and siblings
		if(parentId.pgcateg() != BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
			Iterator<BTreeEntry> ite = parent.iterator();
			while(ite.hasNext()) {
				BTreeEntry e = ite.next();
				if(e.getLeftChild().equals(page.getId())) {
					rightEntry = e;
					break;
				}
				else if(e.getRightChild().equals(page.getId())) {
					leftEntry = e;
				}
			}
		}

		if(page.getId().pgcateg() == BTreePageId.LEAF) {
			handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
		}
		else { // BTreePageId.INTERNAL
			handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
		}
	}

	/**
	 * Handle the case when a leaf page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples, redistribute those tuples.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page which is less than half full
	 * @param parent - the parent of the leaf page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeLeafPages(TransactionId, Map, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage,  BTreeEntry, boolean)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page,
			BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
			throws DbException, IOException, TransactionAbortedException {
		// 根据 page 关联的左右条目获取 page 的左右兄弟页面
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if(rightEntry != null) rightSiblingId = rightEntry.getRightChild();

		// 判断叶子页是合并还是“偷取”
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		// 先判断左兄弟，再判断右兄弟
		if(leftSiblingId != null) {
			BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			// 如果兄弟也“很空”，则合并
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			// 如果兄弟有多余，则“偷取”
			else {
				stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
			}
		}
		else if(rightSiblingId != null) {
			BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
			}
		}
	}

	/**
	 * 从兄弟节点中窃取元组并复制到给定的页面，使得两个页面都至少半满。<br>
	 * 同时更新父节点中的条目，使得关键字与右侧节点的第一个元组的关键字字段匹配。<br>
	 * Steal tuples from a sibling and copy them to the given page so that both pages are at least
	 * half full.  Update the parent's entry so that the key matches the key field of the first
	 * tuple in the right-hand page.
	 *
	 * @param page - the leaf page which is less than half full<br>当前不足半满的叶子页
	 * @param sibling - the sibling which has tuples to spare<br>拥有多余元组可以被窃取的兄弟页
	 * @param parent - the parent of the two leaf pages<br>这两个叶子页的父节点
	 * @param entry - the entry in the parent pointing to the two leaf pages<br>父节点中指向这两个叶子页的条目
	 * @param isRightSibling - whether the sibling is a right-sibling<br>指示 sibling 是否为右兄弟
	 *
	 * @throws DbException
	 */
	public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
			BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
		// some code goes here
        //
        // Move some of the tuples from the sibling to the page so
		// that the tuples are evenly distributed. Be sure to update
		// the corresponding parent entry.

		// 计算需要从兄弟节点移动到当前节点（page）的元组数量
		// (page 元组数 + sibling 元组数) / 2 得到两页平均应有的元组数，用它减去 page 现有元组数得出需插入数
		int numInsert = (page.getNumTuples() + sibling.getNumTuples()) / 2 - page.getNumTuples();

		// 根据是否是右兄弟来决定使用正向还是反向迭代器
		Iterator<Tuple> iterator = isRightSibling ? sibling.iterator() : sibling.reverseIterator();

		// 将指定数量的元组从 sibling 转移到 page
		for (int i = 0; i < numInsert; i++) {
			// 取得下一个元组
			Tuple curTuple = iterator.next();
			// 从 sibling 中删除这个元组
			sibling.deleteTuple(curTuple);
			// 插入到当前 page
			page.insertTuple(curTuple);
		}

		// 使用下一个元组的关键字段更新父节点条目的 key
		entry.setKey(iterator.next().getField(this.keyField));
		// 更新父节点中的条目信息
		parent.updateEntry(entry);
	}

	/**
	 * Handle the case when an internal page becomes less than half full due to deletions.
	 * If one of its siblings has extra entries, redistribute those entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param parent - the parent of the internal page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeInternalPages(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeftInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromRightInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
					throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if(leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if(rightEntry != null) rightSiblingId = rightEntry.getRightChild();

		int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries()/2; // ceiling
		if(leftSiblingId != null) {
			BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
			}
		}
		else if(rightSiblingId != null) {
			BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if(rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
		}
	}

	/**
	 * 从左兄弟节点窃取若干条目并复制到给定的节点，使得两者都至少半满。<br>
	 * 可以将键视为通过父节点条目进行旋转：父节点原先的键被“拉”到右侧节点，<br>
	 * 左侧节点最后的键被“推”到父节点。需要更新相关的父指针。<br>
	 * Steal entries from the left sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
	 * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full<br>当前不足半满的内部页
	 * @param leftSibling - the left sibling which has entries to spare<br>有多余条目可供窃取的左兄弟节点
	 * @param parent - the parent of the two internal pages<br>这两个内部页的父节点
	 * @param parentEntry - the entry in the parent pointing to the two internal pages<br>父节点中指向这两个内部页的条目
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
			BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
		// some code goes here
        // Move some of the entries from the left sibling to the page so
		// that the entries are evenly distributed. Be sure to update
		// the corresponding parent entry. Be sure to update the parent
		// pointers of all children in the entries that were moved.

		// 计算需要从左兄弟节点移动过来的条目数量
		// (page 中现有条目数 + leftSibling 中的条目数) / 2 得到两者平均应有的条目总数
		// 再减去 page 的当前条目数就是需要移动的个数
		int numEntrySteal = (page.getNumEntries() + leftSibling.getNumEntries()) / 2 - page.getNumEntries();

		// 获取左兄弟节点的反向迭代器，从最后一个条目开始往前取
		Iterator<BTreeEntry> iterator = leftSibling.reverseIterator();

		// 先取出左兄弟节点的最后一个条目（反向迭代的第一个条目）
		BTreeEntry curEntry = iterator.next();

		// 将父节点原先指向这两个兄弟页的条目 (parentEntry) “拉”下来，和左兄弟的一个子节点组合
		// 创建一个新的条目（midEntry）：
		//  - 键来自于父节点 (parentEntry.getKey())
		//  - 右子指针为当前左兄弟条目的右子节点 (curEntry.getRightChild())
		//  - 左子指针为当前节点第一个条目的左子指针 (page.iterator().next().getLeftChild())
		// 注意这里是要插入到 page 中
		BTreeEntry midEntry = new BTreeEntry(
				parentEntry.getKey(),
				curEntry.getRightChild(),
				page.iterator().next().getLeftChild()
		);
		page.insertEntry(midEntry);

		// 已经往 page 中插入了一个条目，所以实际还需要移动的条目数减 1
		numEntrySteal -= 1;

		// 循环将左兄弟节点多余的条目转移到当前 page
		for (int i = 0; i < numEntrySteal; i++) {
			// 从左兄弟节点删除当前条目（包含其右子指针）
			leftSibling.deleteKeyAndRightChild(curEntry);
			// 将这个条目插入到 page
			page.insertEntry(curEntry);
			// 移动到下一个要窃取的条目
			curEntry = iterator.next();
		}

		// 此时 curEntry 还没有被转移，它是左兄弟节点的最后一个条目
		// 这个条目里的 key 要“推”到父节点，所以这里要从 leftSibling 中删除它
		leftSibling.deleteKeyAndRightChild(curEntry);

		// 用这个条目的 key 更新父节点中的 parentEntry
		parentEntry.setKey(curEntry.getKey());
		parent.updateEntry(parentEntry);

		// 将 page、leftSibling、parent 标记到脏页集合中
		dirtypages.put(page.getId(), page);
		dirtypages.put(leftSibling.getId(), leftSibling);
		dirtypages.put(parent.getId(), parent);

		// 更新刚才在 page 和 leftSibling 中被修改过的子指针的父指针信息
		updateParentPointers(tid, dirtypages, page);
		updateParentPointers(tid, dirtypages, leftSibling);
	}

	/**
	 * 从右兄弟节点窃取若干条目并复制到给定的节点，使得两者都至少半满。<br>
	 * 可以将键视为通过父节点条目进行旋转：父节点原先的键被“拉”到左侧节点，<br>
	 * 右侧节点最前的键被“推”到父节点。需要更新相关的父指针。<br>
	 * Steal entries from the right sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
	 * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full<br>当前不足半满的内部页
	 * @param rightSibling - the right sibling which has entries to spare<br>有多余条目可供窃取的右兄弟节点
	 * @param parent - the parent of the two internal pages<br>这两个内部页的父节点
	 * @param parentEntry - the entry in the parent pointing to the two internal pages<br>父节点中指向这两个内部页的条目
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	public void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
			BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
		// some code goes here
        // Move some of the entries from the right sibling to the page so
		// that the entries are evenly distributed. Be sure to update
		// the corresponding parent entry. Be sure to update the parent
		// pointers of all children in the entries that were moved.

        // 计算需要从右兄弟节点移动到当前页的条目数量
		// (page 中现有条目数 + rightSibling 中的条目数) / 2 得到平均条目数
		// 减去当前页已有条目数就是要移动的数量
		int numEntrySteal = (page.getNumEntries() + rightSibling.getNumEntries()) / 2 - page.getNumEntries();

		// 获取右兄弟节点的正向迭代器，从第一个条目开始
		Iterator<BTreeEntry> iterator = rightSibling.iterator();

		// 先取出右兄弟节点的第一个条目
		BTreeEntry curEntry = iterator.next();

		// 将父节点原先指向这两个兄弟页的条目 (parentEntry) “拉”下来，
		// 组合成新的条目并插入当前 page：
		//  - 键来自于父节点 (parentEntry.getKey())
		//  - 左子指针为当前 page 最后一个条目的右子指针
		//  - 右子指针为刚从右兄弟节点拿到条目的左子指针 (curEntry.getLeftChild())
		BTreeEntry midEntry = new BTreeEntry(
				parentEntry.getKey(),
				page.reverseIterator().next().getRightChild(),
				curEntry.getLeftChild()
		);
		page.insertEntry(midEntry);

		// 已经向 page 插入了一个条目，所以还需要移动的数目减 1
		numEntrySteal -= 1;

		// 循环移动剩余需要的条目
		for (int i = 0; i < numEntrySteal; i++) {
			// 从右兄弟节点删除当前条目（包含其左子指针）
			rightSibling.deleteKeyAndLeftChild(curEntry);
			// 将条目插入到当前 page
			page.insertEntry(curEntry);
			// 获取下一个需要移动的条目
			curEntry = iterator.next();
		}

		// 此时 curEntry 还未转移，是右兄弟节点的下一个条目
		// 这个条目里的 key “推”到父节点，用它更新父节点中的 parentEntry
		rightSibling.deleteKeyAndLeftChild(curEntry);
		parentEntry.setKey(curEntry.getKey());
		parent.updateEntry(parentEntry);

		// 将 page、rightSibling、parent 标记为脏页
		dirtypages.put(page.getId(), page);
		dirtypages.put(rightSibling.getId(), rightSibling);
		dirtypages.put(parent.getId(), parent);

		// 更新当前页和右兄弟页中被移动条目所影响到的子指针的父指针信息
		updateParentPointers(tid, dirtypages, page);
		updateParentPointers(tid, dirtypages, rightSibling);
	}

	/**
	 * 将两个叶子页合并：把所有元组从右页移动到左页，从父节点中删除相应的条目，<br>
	 * 并在需要时递归地处理父节点在条目不足时的合并。更新兄弟指针，并让右页可供重用。<br>
	 * Merge two leaf pages by moving all tuples from the right page to the left page.
	 * Delete the corresponding key and right child pointer from the parent, and recursively
	 * handle the case when the parent gets below minimum occupancy.
	 * Update sibling pointers as needed, and make the right page available for reuse.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left leaf page<br>左侧叶子页
	 * @param rightPage - the right leaf page<br>右侧叶子页
	 * @param parent - the parent of the two pages<br>这两个叶子页的父节点
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage<br>父节点中指向 leftPage 和 rightPage 的条目
	 * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeLeafPages(TransactionId tid,
							   Map<PageId, Page> dirtypages,
							   BTreeLeafPage leftPage,
							   BTreeLeafPage rightPage,
							   BTreeInternalPage parent,
							   BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {

		// some code goes here
        //
		// Move all the tuples from the right page to the left page, update
		// the sibling pointers, and make the right page available for reuse.
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here

		// 1. 将 rightPage 中的所有元组移动到 leftPage 中
		Iterator<Tuple> rightIterator = rightPage.iterator();
		while (rightIterator.hasNext()) {
			Tuple curTuple = rightIterator.next();
			// 从 rightPage 删除该元组
			rightPage.deleteTuple(curTuple);
			// 将该元组插入到 leftPage 中
			leftPage.insertTuple(curTuple);
		}

		// 2. 更新兄弟指针
		// 如果 rightPage 没有右兄弟，那么合并后 leftPage 的右兄弟就是空
		if (rightPage.getRightSiblingId() == null) {
			leftPage.setRightSiblingId(null);
		} else {
			// 否则让 leftPage 的右兄弟指向原本的 rightPage 的右兄弟
			leftPage.setRightSiblingId(rightPage.getRightSiblingId());
			// 并让该兄弟的左兄弟指针指向 leftPage
			BTreeLeafPage nextPage = (BTreeLeafPage) getPage(
					tid,
					dirtypages,
					rightPage.getRightSiblingId(),
					Permissions.READ_WRITE
			);
			nextPage.setLeftSiblingId(leftPage.getId());
		}

		// 3. 将 rightPage 标记为空页（释放/重用）
		setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());

		// 4. 从父节点中删除指向 (leftPage, rightPage) 的条目（即 parentEntry），
		//    并在必要时向上递归合并
		deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

		// 5. 将这几个修改过的页面加入到脏页列表
		//    （rightPage 即使被清空，有时候也需要更新元数据，所以通常也要放进脏页）
		dirtypages.put(rightPage.getId(), rightPage);
		dirtypages.put(leftPage.getId(), leftPage);
		dirtypages.put(parent.getId(), parent);
	}

	/**
	 * 合并两个内部页：将右侧页面的所有条目移动到左侧页面，并将父节点中对应的键“拉”下来作为连接。<br>
	 * 然后删除父节点中相应的键及右子指针，在必要时递归向上进行合并。同时更新子节点的父指针，
	 * 并让右侧页面可供重用。<br>
	 * Merge two internal pages by moving all entries from the right page to the left page
	 * and "pulling down" the corresponding key from the parent entry.
	 * Delete the corresponding key and right child pointer from the parent, and recursively
	 * handle the case when the parent gets below minimum occupancy.
	 * Update parent pointers as needed, and make the right page available for reuse.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left internal page<br>左侧内部页
	 * @param rightPage - the right internal page<br>右侧内部页
	 * @param parent - the parent of the two pages<br>父节点
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage<br>父节点中指向 leftPage 和 rightPage 的条目
	 * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
	 * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {

		// some code goes here
        //
        // Move all the entries from the right page to the left page, update
		// the parent pointers of the children in the entries that were moved,
		// and make the right page available for reuse
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here

		// 1. 从 rightPage 的迭代器中取出第一个条目 (curEntry)，
		//    准备将 parentEntry.getKey() “拉”下来作为连接条目 (concate) 插入 leftPage
		Iterator<BTreeEntry> rightIterator = rightPage.iterator();
		BTreeEntry curEntry = rightIterator.next();

		// 这里创建的 concate 条目：
		//  - key：来自 parentEntry
		//  - leftChild：leftPage 最后一个条目的右子指针
		//  - rightChild：rightPage 第一个条目的左子指针 (curEntry.getLeftChild())
		// 这就是所谓的“拉”下父节点键进行合并
		BTreeEntry concate = new BTreeEntry(
				parentEntry.getKey(),
				leftPage.reverseIterator().next().getRightChild(),
				curEntry.getLeftChild()
		);
		// 将这个拼接条目插入到左侧页面
		leftPage.insertEntry(concate);

		// 2. 依次将 rightPage 中剩余所有条目移动到 leftPage 中
		//    每次删除 rightPage 中的 curEntry，再插入到 leftPage
		while (rightIterator.hasNext()) {
			rightPage.deleteKeyAndLeftChild(curEntry);
			leftPage.insertEntry(curEntry);
			curEntry = rightIterator.next();
		}
		// 删除 rightPage 中最后一个条目，再插入到 leftPage
		rightPage.deleteKeyAndLeftChild(curEntry);
		leftPage.insertEntry(curEntry);

		// 3. 更新左侧页面里所有子节点的父指针
		updateParentPointers(tid, dirtypages, leftPage);

		// 4. 将 rightPage 标记为空页，释放/重用
		setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());

		// 5. 从父节点中删除指向这两个页面的 parentEntry，
		//    并在必要时进行递归合并
		deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

		// 6. 更新脏页
		dirtypages.put(rightPage.getId(), rightPage);
		dirtypages.put(leftPage.getId(), leftPage);
		dirtypages.put(parent.getId(), parent);
	}

	/**
	 * Method to encapsulate the process of deleting an entry (specifically the key and right child)
	 * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
	 * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
	 * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
	 * merge with a sibling.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the child remaining after the key and right child are deleted
	 * @param parent - the parent containing the entry to be deleted
	 * @param parentEntry - the entry to be deleted
	 * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages,
			BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {

		// delete the entry in the parent.  If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteKeyAndRightChild(parentEntry);
		int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries()/2; // ceiling
		if(parent.getNumEmptySlots() == parent.getMaxEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged
			// page will become the new root
			BTreePageId rootPtrId = parent.getParentId();
			if(rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
		}
		else if(parent.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtypages, parent);
		}
	}

	/**
	 * Delete a tuple from this BTreeFile.
	 * May cause pages to merge or redistribute entries/tuples if the pages
	 * become less than half full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
	 */
	public List<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		Map<PageId, Page> dirtypages = new HashMap<>();

		BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(),
				BTreePageId.LEAF);
		BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		// 如果页面占用率低于最小值，则从其同级页面中获取一些数据元组，或与其中一个同级页面合并。
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if(page.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtypages, page);
		}

        return new ArrayList<>(dirtypages.values());
	}

	/**
	 * Get a read lock on the root pointer page. Create the root pointer page and root page
	 * if necessary.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
		synchronized(this) {
			if(f.length() == 0) {
				// create the root pointer page and the root page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page
		return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
	}

	/**
	 * Get the page number of the first empty page in this BTreeFile.
	 * Creates a new page if none of the existing pages are empty.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the page number of the first empty page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages)
			throws DbException, IOException, TransactionAbortedException {
		// get a read lock on the root pointer page and use it to locate the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		int emptyPageNo = 0;

		if(headerId != null) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			int headerPageCount = 0;
			// try to find a header page with an empty slot
			while(headerPage != null && headerPage.getEmptySlot() == -1) {
				headerId = headerPage.getNextPageId();
				if(headerId != null) {
					headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
					headerPageCount++;
				}
				else {
					headerPage = null;
				}
			}

			// if headerPage is not null, it must have an empty slot
			if(headerPage != null) {
				headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
				int emptySlot = headerPage.getEmptySlot();
				headerPage.markSlotUsed(emptySlot, true);
				emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
			}
		}

		// at this point if headerId is null, either there are no header pages
		// or there are no free slots
		if(headerId == null) {
			synchronized(this) {
				// create the new page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyData = BTreeInternalPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();
				emptyPageNo = numPages();
			}
		}

		return emptyPageNo;
	}

	/**
	 * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
	 * and creates a new page if none are available.  It wipes the page on disk and in the cache and
	 * returns a clean copy locked with read-write permission
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pgcateg - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
	 * @return the new empty page
	 * @see #getEmptyPageNo(TransactionId, Map)
	 * @see #setEmptyPage(TransactionId, Map, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg)
			throws DbException, IOException, TransactionAbortedException {
		// create the new page
		int emptyPageNo = getEmptyPageNo(tid, dirtypages);
		BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

		// write empty page to disk
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
//		if (pgcateg == BTreePageId.LEAF) {
//			rf.write(BTreeLeafPage.createEmptyPageData());
//		} else if (pgcateg == BTreePageId.INTERNAL) {
//			rf.write(BTreeInternalPage.createEmptyPageData());
//		} else if (pgcateg == BTreePageId.ROOT_PTR) {
//			rf.write(BTreeRootPtrPage.createEmptyPageData());
//		} else if (pgcateg == BTreePageId.HEADER) {
//			rf.write(BTreeHeaderPage.createEmptyPageData());
//		} else {
//			rf.write(BTreePage.createEmptyPageData());
//		}
		rf.write(BTreePage.createEmptyPageData());
		rf.close();

		// make sure the page is not in the buffer pool	or in the local cache
		Database.getBufferPool().discardPage(newPageId);
		dirtypages.remove(newPageId);

		return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
	}

	/**
	 * Mark a page in this BTreeFile as empty. Find the corresponding header page
	 * (create it if needed), and mark the corresponding slot in the header page as empty.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param emptyPageNo - the page number of the empty page
	 * @see #getEmptyPage(TransactionId, Map, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	public void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo)
			throws DbException, IOException, TransactionAbortedException {

		// if this is the last page in the file (and not the only page), just
		// truncate the file
		// @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
//		synchronized(this) {
//			if(emptyPageNo == numPages()) {
//				if(emptyPageNo <= 1) {
//					// if this is the only page in the file, just return.
//					// It just means we have an empty root page
//					return;
//				}
//				long newSize = f.length() - BufferPool.getPageSize();
//				FileOutputStream fos = new FileOutputStream(f, true);
//				FileChannel fc = fos.getChannel();
//				fc.truncate(newSize);
//				fc.close();
//				fos.close();
//				return;
//			}
//		}

		// otherwise, get a read lock on the root pointer page and use it to locate
		// the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		BTreePageId prevId = null;
		int headerPageCount = 0;

		// if there are no header pages, create the first header page and update
		// the header pointer in the BTreeRootPtrPage
		if(headerId == null) {
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);

			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			rootPtr.setHeaderId(headerId);
		}

		// iterate through all the existing header pages to find the one containing the slot
		// corresponding to emptyPageNo
		while(headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			prevId = headerId;
			headerId = headerPage.getNextPageId();
			headerPageCount++;
		}

		// at this point headerId should either be null or set with
		// the headerPage containing the slot corresponding to emptyPageNo.
		// Add header pages until we have one with a slot corresponding to emptyPageNo
		while((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			headerPage.setPrevPageId(prevId);
			prevPage.setNextPageId(headerId);

			headerPageCount++;
			prevId = headerId;
		}

		// now headerId should be set with the headerPage containing the slot corresponding to
		// emptyPageNo
		BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
		int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
		headerPage.markSlotUsed(emptySlot, false);
	}

	/**
	 * get the specified tuples from the file based on its IndexPredicate value on
	 * behalf of the specified transaction. This method will acquire a read lock on
	 * the affected pages of the file, and may block until the lock can be
	 * acquired.
	 *
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BTreeSearchIterator(this, tid, ipred);
	}

	/**
	 * Get an iterator for all tuples in this B+ tree file in sorted order. This method
	 * will acquire a read lock on the affected pages of the file, and may block until
	 * the lock can be acquired.
	 *
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	public DbFileIterator iterator(TransactionId tid) {
		return new BTreeFileIterator(this, tid);
	}

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	final TransactionId tid;
	final BTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BTreeFileIterator(BTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		curp = f.findLeafPage(tid, root, null);
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 *
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext())
			it = null;

		while (it == null && curp != null) {
			BTreePageId nextp = curp.getRightSiblingId();
			if(nextp == null) {
				curp = null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
				if (!it.hasNext())
					it = null;
			}
		}

		if (it == null)
			return null;
		return it.next();
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
		curp = null;
	}
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	final TransactionId tid;
	final BTreeFile f;
	final IndexPredicate ipred;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page applicable
	 * for the given predicate operation
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		if(ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
				|| ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, root, ipred.getField());
		}
		else {
			curp = f.findLeafPage(tid, root, null);
		}
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples matching
	 * the predicate or from the next page by following the right sibling pointer.
	 *
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException,
	NoSuchElementException {
		while (it != null) {

			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if(ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if(ipred.getOp() == Op.EQUALS &&
						t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if(nextp == null) {
				return null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
	}
}
