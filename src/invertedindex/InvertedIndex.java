package invertedindex;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.ComparableComparator;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.recman.CacheRecordManager;
import neustore.base.ByteArray;
import neustore.base.DBBuffer;
import neustore.base.DBBufferReturnElement;
import neustore.base.DBIndex;
import neustore.base.Data;
import neustore.base.FloatData;
import neustore.base.FloatDataComparatorDes;
import neustore.base.IntKey;
import neustore.base.Key;
import neustore.base.KeyData;
import neustore.base.LRUBuffer;
import neustore.heapfile.HeapFilePage;


public class InvertedIndex extends DBIndex {
	
	public static float maxTR = 5;

	protected CacheRecordManager cacheRecordManager;

	protected long          recid;

	protected BTree         btree;

	protected int 		  pageSize;

	private int 		  numRecs;

	protected Key sampleKey;

	protected Data sampleData;

	protected Hashtable invertedlists;

	protected int count;

	public InvertedIndex(DBBuffer _buffer, String filename, boolean isCreate, int pagesize, int buffersize, Key _sampleKey, Data _sampleData) throws IOException {
		super( _buffer, filename, isCreate );
		sampleKey = _sampleKey;
		sampleData = _sampleData;

		int cachesize = buffersize;
		// RecordManagerFactory.createRecordManager()����һ��String��Ϊ�������ò�����Ϊ���������������ݿ��ļ����ֵ�ǰ׺��
		cacheRecordManager = new CacheRecordManager(RecordManagerFactory.createRecordManager(filename), cachesize, true);
		pageSize = pagesize;
		/**
		 * �������RecordManager.insert()�������ݣ����ݴ洢��ʽΪid+���л��Ķ���
		 * �����BTree��HTree.insert()�������ݣ����ݱ�����ʽΪkey+value������HashTable;
		 */
		if ( !isCreate ) {
			// ������ͨ��������ȡ�����ݵ�id��������м�¼��id����ô����fetch(id)�ķ���
			// BTree.load()����Ҳ���Ի��һ��BTree�Ķ��󣬸÷�����Ҫ����������һ����RecordManager����һ����BTree��idֵ��
			// ������һ��BTree����ʹ�õ��Ǿ�̬��BTree.createInstance()���÷�����������������һ����Recordmanager����һ����Comparator(�����ܹ��Լ�ֵ���бȽ�)
			recid = cacheRecordManager.getNamedObject( "0" );
			btree = BTree.load( cacheRecordManager, recid );
			//System.out.println("loading btree: " + btree.size());
		} 
		else {
			btree = BTree.createInstance( cacheRecordManager, ComparableComparator.INSTANCE, DefaultSerializer.INSTANCE, DefaultSerializer.INSTANCE, 1000 );
			// jdbm��setNameObject()�������Ǹ������������ͬ��getNameObject(����)��õĶ��Ǹö�����jdbm�е�idֵ��
			cacheRecordManager.setNamedObject( "0", btree.getRecid() );	          
		}

		invertedlists = new Hashtable();
		count = 0;
	}

	public InvertedIndex(DBBuffer _buffer, String filename, boolean isCreate, int pagesize, int buffersize)throws IOException{
		super( _buffer, filename, isCreate );
		sampleKey = new IntKey(0);
		sampleData = new FloatData(0);

		int cachesize = buffersize;
		cacheRecordManager = new CacheRecordManager(RecordManagerFactory.createRecordManager(filename), cachesize, true);
		pageSize = pagesize;

		//���������ǲ���ȱ�ٴ��룿BTree�Ĵ����أ�
	}

	protected void readIndexHead (byte[] indexHead) {
		ByteArray ba = new ByteArray( indexHead, true );
		try {
			numRecs = ba.readInt();

		} catch (IOException e) {}
	}
	protected void writeIndexHead (byte[] indexHead) {
		ByteArray ba = new ByteArray( indexHead, false );
		try {
			ba.writeInt(numRecs);

		} catch (IOException e) {}
	}
	protected void initIndexHead() {
		numRecs = 0;

	}


	public int numRecs() { return numRecs; }

	protected HeapFilePage readPostingListPage( long pageID ) throws IOException {
		DBBufferReturnElement ret = buffer.readPage(file, pageID);
		HeapFilePage thePage = null;
		if ( ret.parsed ) {
			thePage = (HeapFilePage)ret.object;
		}
		else {
			thePage = new HeapFilePage(pageSize, sampleKey, sampleData);
			thePage.read((byte[])ret.object);
		}
		return thePage;
	}

	/**
	 * ���������ṹ��
	 * wordID | (ArrayList<KeyData<docID,weight>>) data |....
	 * wordID | (ArrayList<KeyData<docID,weight>>) data |....
	 * @param docID
	 * @param document
	 * @param invertedindex
	 * @throws IOException
	 */

	public void insertDocument(int docID, Vector document, Hashtable invertedindex) throws IOException{
		IntKey key = new IntKey(docID);
		for(int i = 0; i < document.size(); i++){
			KeyData keydata = (KeyData)document.get(i);
			IntKey wordID = (IntKey)keydata.key;
			FloatData weight = (FloatData)keydata.data;
			KeyData rec = new KeyData(key, weight);
			if(invertedindex.containsKey(wordID.key)){
				ArrayList list = (ArrayList)invertedindex.get(wordID.key);
				list.add(rec);
			}
			else{
				ArrayList list = new ArrayList();
				list.add(rec);
				invertedindex.put(wordID.key, list);
			}
		}
	}

	public Vector store(int treeid, Hashtable invertedindex, int num) throws IOException{
		Vector pseudoDoc = new Vector();
		load(treeid);
		Iterator iter = invertedindex.keySet().iterator();
		while(iter.hasNext()){
			int wordID = (Integer)iter.next();
			ArrayList list = (ArrayList)invertedindex.get(wordID);
			long newPageID = allocate();
			Object var = btree.insert(wordID, newPageID, false);
			if(var != null){
				System.out.println("Btree insertion error: duplicate keys.");
				System.exit(-1);
			}
			HeapFilePage newPage = new HeapFilePage(pageSize, sampleKey, sampleData);

			FloatData weight = storelist(list, newPage, newPageID, num);
			IntKey key = new IntKey(wordID);
			KeyData rec = new KeyData(key, weight);
			pseudoDoc.add(rec);
		}
		cacheRecordManager.commit();
		return pseudoDoc;
	}


	public void commit()throws IOException{
		cacheRecordManager.commit();
	}

	private FloatData storelist(ArrayList list, HeapFilePage newPage, long newPageID, int num) throws IOException{
		FloatData weight = new FloatData(Float.NEGATIVE_INFINITY,Float.POSITIVE_INFINITY);

		for(int j = 0; j < list.size(); j++){
			KeyData rec = (KeyData)list.get(j);
			IntKey key = (IntKey)rec.key;
			FloatData data = (FloatData)rec.data;
			
			weight.data  = Math.max(weight.data, data.data);
			weight.data2 = Math.min(weight.data2, data.data2);
			
			int availableByte = newPage.getAvailableBytes();
			if(availableByte < key.size() + data.size()){

				long nextPageID = allocate();
				newPage.setNextPageID(nextPageID);
				buffer.writePage(file, newPageID, newPage);
				newPageID = nextPageID;
				newPage = new HeapFilePage(pageSize, sampleKey, sampleData);
			}
			newPage.insert(key, data);
		}
		if(list.size() < num) weight.data2 = 0;
		buffer.writePage(file, newPageID, newPage);
		return weight;
	}

	public Hashtable textRelevancy(Vector qwords) throws IOException{

		Hashtable filter = new Hashtable();		

		for(int j = 0; j < qwords.size(); j++){
			int word = (Integer)qwords.get(j);
			ArrayList list = readPostingList(word);
			for(int i = 0; i < list.size(); i++){
				KeyData rec = (KeyData)list.get(i);
				IntKey docID = (IntKey)rec.key;
				FloatData weight = (FloatData)rec.data;
				
				if(filter.containsKey(docID.key)){
					FloatData w = (FloatData)filter.get(docID.key);
					w.data  = w.data + weight.data;
					w.data2 = w.data2+weight.data2;
					filter.put(docID.key, w);
				}
				else			
					filter.put(docID.key, weight);
			}
		}
		return filter;
	}

	
	public ArrayList readPostingList(int wordID) throws IOException{
		ArrayList list = new ArrayList();
		Object var = btree.find(wordID);
		if(var == null){
			//System.out.println("Posting List not found " + wordID);
			//System.exit(-1);
			return list;
		}
		else{
			long firstPageID = (Long)var;

			while(firstPageID != -1){
				//System.out.println(" page " + firstPageID);
				HeapFilePage thePage = readPostingListPage(firstPageID);
				for(int i = 0; i < thePage.numRecs(); i++){
					KeyData rec = thePage.get(i);
					list.add(rec);
				}
				//System.out.println("size " + thePage.numRecs());
				firstPageID = thePage.getNextPageID();
			}

		}
		return list;
	}
	

	
	public int[] getIOs(){
		return buffer.getIOs();
	}

	

	public BTree getBtree(){
		return btree;
	}
	public void create(int treeid) throws IOException{

		String BTREE_NAME = String.valueOf(treeid);
		recid = cacheRecordManager.getNamedObject( BTREE_NAME );
		if ( recid != 0 ) {
			System.out.println("Creating an existing btree: " + treeid);
			System.exit(-1);
		} 
		else {
			btree = BTree.createInstance( cacheRecordManager, ComparableComparator.INSTANCE, DefaultSerializer.INSTANCE, DefaultSerializer.INSTANCE, 1000 );
			cacheRecordManager.setNamedObject( BTREE_NAME, btree.getRecid() );	          
		}
	}

	public void load(int treeid)throws IOException{
		String BTREE_NAME = String.valueOf(treeid);
		recid = cacheRecordManager.getNamedObject( BTREE_NAME );
		if ( recid != 0 ) {
			recid = cacheRecordManager.getNamedObject( BTREE_NAME );
			btree = BTree.load( cacheRecordManager, recid );
			//System.out.println("loading btree: " + btree.size());
		} 
		else {
			System.out.println("Failed loading btree: " + treeid);
			System.exit(-1);          
		}
	}

	public int getBtreeSize(){
		return btree.size();
	}


}
