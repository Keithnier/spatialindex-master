##代码阅读记录
<ol>
<li>文档关键字的关联:代码使用jdbm记录了文档id以及各个关键字在文档中的权重，并使用BTree进行管理。
从代码<b>spatialindex/rtree/BtreeStore.java</b>中可以组织处相关记录文件的格式:
<pre>
               /**
		 *  B树data文件格式
		 *  id,?,?,wordID weight,wordID weight,...\n
		 *  id,?,?,wordID weight,wordID weight,...\n
		 *  ...
		 */
</pre>
其中,?表示两个double类型的变量，暂时不知道具体作用是什么，后面会补充。
</li>
</ol>