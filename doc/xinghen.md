##代码阅读记录
<ol>
<li>文档关键字的关联:代码使用jdbm记录了文档id以及各个关键字在文档中的权重，并使用BTree进行管理。
从代码<b>spatialindex/rtree/BtreeStore.java</b>中可以组织处相关记录文件的格式:
<pre>
               /**
		 *  B树data文件格式
		 *  id,floatX,floatY,wordID weight,wordID weight,...\n
		 *  id,floatX,floatY,wordID weight,wordID weight,...\n
		 *  ...
		 */
</pre>
其中,两个float类型的变量，用来表示地理坐标，暂时不知道具体作用是什么，后面会补充。
</li>
<li><b>invertedindex/InvertedIndex.java</b>在建立倒排索引的过程中，会读取1中的文件。并利用该文件建立倒排列表。下面给出倒排索引结构：
<pre>倒排索引结构：
    wordID | (ArrayList<KeyData<docID,weight>>) data |...
    wordID | (ArrayList<KeyData<docID,weight>>) data |...
    ...
</pre>
 倒排索引本身是一个Hashtable，其中键为关键词的id，值为一个ArrayList，且该链表中每一项是一个二元组(文档id，关键词权重)。
</li>
<li>输入文档格式为1中的格式，在BTreeStore中会建立BTree管理文档集，之后再InvertedIndex中会读取文档，建立倒排索引。</li>
<li>IRTree的排序规则，依据分数制。具体代码如下:
<pre>score = (1-alpha)*(1-dist)+alpha*trs;</pre>
其中alpha是权重调节因数，dist是查询点和实际结点的距离，由getMinimumDistance()返回，trs是TF(词频) / 5所得。
</li>
</ol>
