##�����Ķ���¼
<ol>
<li>�ĵ��ؼ��ֵĹ���:����ʹ��jdbm��¼���ĵ�id�Լ������ؼ������ĵ��е�Ȩ�أ���ʹ��BTree���й���
�Ӵ���<b>spatialindex/rtree/BtreeStore.java</b>�п�����֯����ؼ�¼�ļ��ĸ�ʽ:
<pre>
               /**
		 *  B��data�ļ���ʽ
		 *  id,?,?,wordID weight,wordID weight,...\n
		 *  id,?,?,wordID weight,wordID weight,...\n
		 *  ...
		 */
</pre>
����,?��ʾ����double���͵ı�������ʱ��֪������������ʲô������Ჹ�䡣
</li>
<li><b>invertedindex/InvertedIndex.java</b>�ڽ������������Ĺ����У����ȡ1�е��ļ��������ø��ļ����������б�����������������ṹ��
<pre>���������ṹ��
    wordID | (ArrayList<KeyData<docID,weight>>) data |...
    wordID | (ArrayList<KeyData<docID,weight>>) data |...
    ...
</pre>
 ��������������һ��Hashtable�����м�Ϊ�ؼ��ʵ�id��ֵΪһ��ArrayList���Ҹ�������ÿһ����һ����Ԫ��(�ĵ�id���ؼ���Ȩ��)��
</li>
<li>�����ĵ���ʽΪ1�еĸ�ʽ����BTreeStore�лὨ��BTree�����ĵ�����֮����InvertedIndex�л��ȡ�ĵ�����������������</li>
<li>IRTree������������ݷ����ơ������������:
<pre>score = (1-alpha)*(1-dist)+alpha*trs;</pre>
����alpha��Ȩ�ص���������dist�ǲ�ѯ���ʵ�ʽ��ľ��룬��getMinimumDistance()���أ�trs��TF(��Ƶ) / 5���á�
</li>
</ol>