package simpledb.index;

import simpledb.storage.Field;
import simpledb.storage.RecordId;

import java.io.Serializable;

/**
 * 表示 B+ 树内部节点中的一个条目。每个条目包含一个键和两个子节点页面的 ID。<br>
 * 被用于 BTreeInternalPage，存储索引信息，存储一个键值和对应的两个子页面的指针。<br>
 * Each instance of BTreeEntry stores one key and two child page ids. It is used
 * by BTreeInternalPage as an abstraction to iterate through the entries stored inside. 
 * All of the entries or tuples in the left child page should be less than or equal to 
 * the key, and all of the entries or tuples in the right child page should be greater 
 * than or equal to the key.<br>
 * 
 * Note that updating a BTreeEntry does not actually change the data stored on the page 
 * identified by its recordId. After updating a BTreeEntry object, you must call 
 * BTreeInternalPage.updateEntry() in order for the changes to take effect.
 *
 * @see BTreeInternalPage
 * @see BTreeInternalPage#updateEntry(BTreeEntry)
 *
 */
public class BTreeEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The key of this entry
	 * */
	private Field key;

	/**
	 * The left child page id
	 * leftChild 所指向的页面上的元组都比该条目上的键小。
	 * */
	private BTreePageId leftChild;

	/**
	 * The right child page id
	 * rightChild 所指向的页面上的元组都比该条目上的键大。
	 * */
	private BTreePageId rightChild;

	/**
	 * The record id of this entry
	 * 记录该条目在数据库中存储的实际位置。
	 * */
	private RecordId rid; // null if not stored on any page

	/**
	 * Constructor to create a new BTreeEntry
	 * @param key - the key
	 * @param leftChild - page id of the left child
	 * @param rightChild - page id of the right child
	 */
	public BTreeEntry(Field key, BTreePageId leftChild, BTreePageId rightChild) {
		this.key = key;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
	}
	
	/**
	 * @return the key
	 */
	public Field getKey() {
		return key;
	}
	
	/**
	 * @return the left child page id
	 */
	public BTreePageId getLeftChild() {
		return leftChild;
	}
	
	/**
	 * @return the right child page id
	 */
	public BTreePageId getRightChild() {
		return rightChild;
	}
	
	/**
	 * @return the record id of this entry, representing the location of this entry
	 * in a BTreeFile. May be null if this entry is not stored on any page in the file
	 */
	public RecordId getRecordId() {
		return rid;
	}
	
	/**
	 * Set the key for this entry. Note that updating a BTreeEntry does not 
	 * actually change the data stored on the page identified by its recordId.  After
	 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
	 * it to take effect.
	 * @param key - the new key
	 * @see BTreeInternalPage#updateEntry(BTreeEntry)
	 */
	public void setKey(Field key) {
		this.key = key;
	}
	
	/**
	 * Set the left child id for this entry.  Note that updating a BTreeEntry does not 
	 * actually change the data stored on the page identified by its recordId.  After
	 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
	 * it to take effect.
	 * @param leftChild - the new left child
	 * @see BTreeInternalPage#updateEntry(BTreeEntry)
	 */
	public void setLeftChild(BTreePageId leftChild) {
		this.leftChild = leftChild;
	}
	
	/**
	 * Set the right child id for this entry.  Note that updating a BTreeEntry does not 
	 * actually change the data stored on the page identified by its recordId.  After
	 * calling this method, you must call BTreeInternalPage.updateEntry() in order for
	 * it to take effect.
	 * @param rightChild - the new right child
	 * @see BTreeInternalPage#updateEntry(BTreeEntry)
	 */
	public void setRightChild(BTreePageId rightChild) {
		this.rightChild = rightChild;
	}
	
	/**
	 * set the record id for this entry
	 * @param rid - the new record id
	 */
	public void setRecordId(RecordId rid) {
		this.rid = rid;
	}
	
	/**
	 * Prints a representation of this BTreeEntry
	 */
	public String toString() {
		return "[" + leftChild.getPageNumber() + "|" + key + "|" + rightChild.getPageNumber() + "]";
	}
	
}

