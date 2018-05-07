package spatialindex.rtree;

import invertedindex.InvertedIndex;
import neustore.base.FloatData;
import neustore.base.LRUBuffer;
import query.Query;
import spatialindex.spatialindex.*;
import spatialindex.storagemanager.*;
import util.Constants;

import java.io.*;
import java.util.*;

public class IRTree extends RTree {

    //protected  Hashtable numberofpoint = new Hashtable();

    protected InvertedIndex iindex;
    protected int count;


    public IRTree(PropertySet ps, IStorageManager sm, boolean isCreate) throws IOException {
        super(ps, sm);
        int buffersize = (Integer) ps.getProperty("BufferSize");
        int pagesize = (Integer) ps.getProperty("PageSize");
        String file = (String) ps.getProperty("FileName");
        LRUBuffer buffer = new LRUBuffer(buffersize, pagesize);
        iindex = new InvertedIndex(buffer, file + ".iindex", isCreate, pagesize, buffersize);
    }

    public void buildInvertedIndex(BtreeStore docstore) throws Exception {
        count = 0;
        Node n = readNode(m_rootID);
//        System.err.println(m_rootID);
        post_traversal_iindex(n, docstore);
    }

    private Vector post_traversal_iindex(Node n, BtreeStore docstore) throws Exception {
        if (n.isLeaf()) {
            Hashtable invertedindex = new Hashtable();
            n.numOfleaf = n.m_children;
            writeNode(n);
            iindex.create(n.m_identifier);
            int child;
            for (child = 0; child < n.m_children; child++) {
                int docID = n.m_pIdentifier[child];
                Vector document = docstore.getDoc(docID);
                if (document == null) {
                    System.out.println("Can't find document " + docID);
                    System.exit(-1);
                }
                iindex.insertDocument(docID, document, invertedindex);
            }
            return iindex.store(n.m_identifier, invertedindex, n.m_children);
        } else {
            Hashtable invertedindex = new Hashtable();
            iindex.create(n.m_identifier);
            System.out.println("processing index node " + n.m_identifier);
            System.out.println("level " + n.m_level);
            int child;
            for (child = 0; child < n.m_children; child++) {
                Node nn = readNode(n.m_pIdentifier[child]);
                Vector pseudoDoc = post_traversal_iindex(nn, docstore);
                n.numOfleaf = n.numOfleaf + nn.numOfleaf;
                int docID = n.m_pIdentifier[child];
                iindex.insertDocument(docID, pseudoDoc, invertedindex);
                count++;
                System.out.println(count + "/" + m_stats.getNumberOfNodes());
            }
            writeNode(n);
            return iindex.store(n.m_identifier, invertedindex, n.m_children);
        }
    }


    public void close() throws IOException {
        flush();
        iindex.close();
    }

    /**
     * @param qwords
     * @param qpoint
     * @param topk
     * @param expected_ob
     * @param alpha
     * @param threshold
     * @return
     * @throws Exception
     */
    public Line determine_rank(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double threshold) throws Exception {

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        // NNEntry(entry, minDist, level)
        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());
                // 加载B树
                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    if ((threshold != 0) && (candidate_rank > threshold)) return null;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob)
                    return new Line(first.normalized_dist, first.normalized_simi, alpha, topk, candidate_rank);

            }
        }

        return null;
    }


    //find all the objects rank higher than the missing object under a specific weight $\vector(w)$
    public LinkedList<Candidatevector> find_higher(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double threshold) throws Exception {

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        LinkedList<Candidatevector> q = new LinkedList<Candidatevector>();
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {

                if (first.m_pEntry.getIdentifier() == expected_ob)
                    return q;

                q.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }
        }

        return null;
    }


    public void answeringWhyNot_1_no(Vector qwords, Point qpoint, Line line, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatepoint> candidatepointList = new ArrayList<Candidatepoint>(1000);

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double x = 0.0;
        int rank = 0;
        int i = 0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                rank++;
                if (first.m_minDist == line.b) break;

                if ((first.normalized_dist <= line.a) && (first.normalized_simi > line.b)) {
                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        p.in--;
                        candidatepointList.set(i, p);
                    } else {
                        p.in = -1;
                        candidatepointList.add(p);
                    }
                }

            }

        }

        queue.clear();
        nnc = new NNComparator();
        x = 0.0;
        i = 0;

        n = null;
        nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = 1 - dist;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {

                if (first.m_minDist == line.a) break;
                if ((first.normalized_dist > line.a) && (first.normalized_simi <= line.b)) {
                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        p.out++;
                        candidatepointList.set(i, p);
                    } else {
                        p.out = 1;
                        candidatepointList.add(p);
                    }
                }

            }

        }

        Candidatepoint[] points = new Candidatepoint[candidatepointList.size()];
        //System.out.println("length  " +  points.length);
        candidatepointList.toArray(points);

        Arrays.sort(points, new CandidatepointComparator());

    /*
    for(int iii = 0; iii<points.length; iii++)
    {
    System.out.print(points[iii].location_xcoordinate + "  ");
    }
    System.out.println();
    */

        double p = 0.0;
        double y = 0.0;
        double w1 = 0.0;
        double w2 = 0.0;
        double delta_k = 0.0;
        double delta_w = 0.0;
        double base_k = (double) line.initial_rank - line.k;
        double base_w = Math.sqrt(1 + line.alpha * line.alpha + (1 - line.alpha) * (1 - line.alpha));
        double penalty = penalty_alpha;


        int refined_k = line.initial_rank;

        //System.out.println("initial rank: "+ refined_k);

        double refined_w = line.alpha;
        //System.out.println("length: "+ points.length);
        for (int j = 0; j < points.length; j++) {
            rank = rank + points[j].in;
            if (points[j].location_xcoordinate == 0) continue;
            y = (c - points[j].location_xcoordinate * line.a) / line.b;  // if similarity equals to 0??????
            if (y == 0) continue;

            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            if (rank <= line.k) delta_k = 0;
            else delta_k = rank - line.k;

            delta_w = Math.sqrt((w1 + line.alpha - 1) * (w1 + line.alpha - 1) + (w2 - line.alpha) * (w2 - line.alpha));

            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }

            // System.out.println(w2+" "+penalty+" "+rank+" "+determine_rank(qwords, qpoint, 1,1568434 ,w2, 0).initial_rank);
            rank = rank + points[j].out;

        }
        System.out.println("The refined query is a Top-" + refined_k + " query with weight " + refined_w);
        //System.out.println("penalty:" + penalty);

    }


    public void answeringWhyNot_11(Vector qwords, Point qpoint, Line line, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatepoint> candidatepointList = new ArrayList<Candidatepoint>(1000);

        LinkedList queue = new LinkedList();
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double x = 0.0;
        int rank = 0;
        int i = 0;
        int ccccccc = 0;
        int cc = 0;
        Node n = null;
        Data nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));
        double x0 = c / (line.a + line.b * line.alpha / (1 - line.alpha));
        Candidatepoint pp = new Candidatepoint(x0);
        candidatepointList.add(pp);

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);

                    if (var == null) {

                        continue;
                    }

                    double trsdata = ((FloatData) var).data;
                    double trsdata2 = ((FloatData) var).data2;

                    trsdata = trsdata / InvertedIndex.maxTR;
                    trsdata2 = trsdata2 / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double maxdist = 1 - nnc.getMinimumDistance(qpoint, e) / ooo;
                    double mindist = 1 - getMaximumDistance(qpoint, e) / ooo;


                    if ((mindist > line.a) && (trsdata2 > line.b)) continue;
                    if ((maxdist < line.a) && (trsdata < line.b)) continue;

                    double score = (1 - line.alpha) * maxdist + line.alpha * trsdata;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, mindist, trsdata);
                    queue.add(e2);
                }
            } else {
                ccccccc++;
                if (first.normalized_dist == line.a || first.normalized_simi == line.b) cc++;
                if ((first.normalized_dist < line.a) && (first.normalized_simi > line.b)) {

                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        if (x >= x0) p.in--;
                        else p.out++;
                        candidatepointList.set(i, p);
                    } else {
                        if (x >= x0) p.in = -1;
                        else p.out = 1;
                        candidatepointList.add(p);
                    }
                } else if ((first.normalized_dist > line.a) && (first.normalized_simi < line.b)) {

                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        if (x < x0) p.in--;
                        else p.out++;
                        candidatepointList.set(i, p);
                    } else {
                        if (x < x0) p.in = -1;
                        else p.out = 1;
                        candidatepointList.add(p);
                    }
                }

            }

        }


        //System.out.println(ccccccc);
        //System.out.println(cc);


        Candidatepoint[] points = new Candidatepoint[candidatepointList.size()];

        candidatepointList.toArray(points);
        Arrays.sort(points, new CandidatepointComparator());
        double p = 0.0;
        double y = 0.0;
        double w1 = 0.0;
        double w2 = 0.0;
        double delta_k = 0.0;
        double delta_w = 0.0;
        double base_k = (double) line.initial_rank - line.k;
        double base_w = Math.sqrt(1 + line.alpha * line.alpha + (1 - line.alpha) * (1 - line.alpha));
        double penalty = penalty_alpha;

    /*
    System.out.println("length: "+ points.length);
    for(int iii = 0; iii<points.length; iii++)
    {
    System.out.print(points[iii].location_xcoordinate + "  ");
    }
    System.out.println();
    */

        int refined_k = line.initial_rank;
        double refined_w = line.alpha;

        int index0;
        for (index0 = 0; index0 < points.length; index0++)
            if (points[index0].location_xcoordinate == x0) break;


        rank = line.initial_rank + Math.abs(points[index0].out);
        for (int j = index0 + 1; j < points.length; j++) {
            rank = rank + points[j].in;
            y = (c - points[j].location_xcoordinate * line.a) / line.b;  // if similarity equals to 0??????

            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            if (rank <= line.k) delta_k = 0;
            else delta_k = rank - line.k;

            delta_w = Math.sqrt((w1 + line.alpha - 1) * (w1 + line.alpha - 1) + (w2 - line.alpha) * (w2 - line.alpha));

            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }
            rank = rank + points[j].out;
        }

        rank = line.initial_rank + Math.abs(points[index0].in);
        for (int j = index0 - 1; j >= 0; j--) {
            rank = rank + points[j].in;
            y = (c - points[j].location_xcoordinate * line.a) / line.b;  // if similarity equals to 0??????

            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            if (rank <= line.k) delta_k = 0;
            else delta_k = rank - line.k;

            delta_w = Math.sqrt((w1 + line.alpha - 1) * (w1 + line.alpha - 1) + (w2 - line.alpha) * (w2 - line.alpha));

            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }
            rank = rank + points[j].out;
        }


        System.out.println("The refined query is a Top-" + refined_k + " query with weight " + refined_w);
        System.out.println("penalty:" + penalty);

    }


    public void answeringWhyNot_111(Vector qwords, Point qpoint, Line line, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatepoint> candidatepointList = new ArrayList<Candidatepoint>(1000);

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double x = 0.0;
        int rank = 0;
        int i = 0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));


        //only consider similarity
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                rank++;
                if (first.m_minDist == line.b) break;

                if ((first.normalized_dist < line.a) && (first.normalized_simi > line.b)) {
                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        p.in--;
                        candidatepointList.set(i, p);
                    } else {
                        p.in = -1;
                        candidatepointList.add(p);
                    }
                }

            }

        }

        queue.clear();
        nnc = new NNComparator();
        x = 0.0;
        i = 0;

        n = null;
        nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));


        //only consider distance
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = 1 - dist;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {

                if (first.m_minDist == line.a) break;
                if ((first.normalized_dist > line.a) && (first.normalized_simi <= line.b)) {
                    x = (line.b - first.normalized_simi) * c / (line.b * first.normalized_dist - line.a * first.normalized_simi);
                    Candidatepoint p = new Candidatepoint(x);
                    i = candidatepointList.indexOf(p);
                    if (i != -1) {
                        p = candidatepointList.get(i);
                        p.out++;
                        candidatepointList.set(i, p);
                    } else {
                        p.out = 1;
                        candidatepointList.add(p);
                    }
                }

            }

        }


        double x0 = c / (line.a + line.b * line.alpha / (1 - line.alpha));
        int testx0 = 0;

        Candidatepoint pp = new Candidatepoint(x0);
        testx0 = candidatepointList.indexOf(pp);
        if (testx0 == -1) candidatepointList.add(pp);


        Candidatepoint[] points = new Candidatepoint[candidatepointList.size()];
        System.out.println("length  " + points.length);
        candidatepointList.toArray(points);

        Arrays.sort(points, new CandidatepointComparator());


        int index0 = 0;
        for (index0 = 0; index0 < points.length; index0++)
            if (points[index0].location_xcoordinate == x0) break;


        double p = 0.0;
        double y = 0.0;
        double w1 = 0.0;
        double w2 = 0.0;
        double delta_k = 0.0;
        double delta_w = 0.0;
        double base_k = (double) line.initial_rank - line.k;
        double base_w = Math.sqrt(1 + line.alpha * line.alpha + (1 - line.alpha) * (1 - line.alpha));
        double penalty = penalty_alpha;


        int refined_k = line.initial_rank;

        //System.out.println("initial rank: "+ refined_k);

        double refined_w = line.alpha;
        //System.out.println("length: "+ points.length);


        rank = line.initial_rank + points[index0].out;
        for (int j = index0; j < points.length; j++) {
            rank = rank + points[j].in;
            if (points[j].location_xcoordinate == 0) continue;
            y = (c - points[j].location_xcoordinate * line.a) / line.b;  // if similarity equals to 0??????
            if (y == 0) continue;

            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            if (rank <= line.k) delta_k = 0;
            else delta_k = rank - line.k;

            delta_w = Math.sqrt((w1 + line.alpha - 1) * (w1 + line.alpha - 1) + (w2 - line.alpha) * (w2 - line.alpha));

            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }

            // System.out.println(w2+" "+penalty+" "+rank+" "+determine_rank(qwords, qpoint, 1,1568434 ,w2, 0).initial_rank);
            rank = rank + points[j].out;

        }

        rank = line.initial_rank - points[index0].in;
        for (int j = index0; j > 0; j--) {
            rank = rank - points[j].out;
            if (points[j].location_xcoordinate == 0) continue;
            //y = (c - points[j].location_xcoordinate*line.a)/line.b;  // if similarity equals to 0??????
            if (x == 0) continue;

            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            if (rank <= line.k) delta_k = 0;
            else delta_k = rank - line.k;

            delta_w = Math.sqrt((w1 + line.alpha - 1) * (w1 + line.alpha - 1) + (w2 - line.alpha) * (w2 - line.alpha));

            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }

            // System.out.println(w2+" "+penalty+" "+rank+" "+determine_rank(qwords, qpoint, 1,1568434 ,w2, 0).initial_rank);
            rank = rank - points[j].in;

        }
        System.out.println("The refined query is a Top-" + refined_k + " query with weight " + refined_w);
        //System.out.println("penalty:" + penalty);

    }


    //algorithm 2_new + sort
    public void answeringWhyNot_22(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double penalty_alpha, double c) throws Exception {

        ArrayList<Candidatevector> candidatevector = new ArrayList<Candidatevector>();
        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double xx = 0.0;
        double yy = 0.0;
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;
        int refined_k = 0;
        double refined_w = 0.0;
        double penalty = penalty_alpha;
        double threshold = 0.0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);

                    if (var == null) {

                        continue;
                    }

                    FloatData trs = (FloatData) var;
                    double trss = (double) trs.data / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trss;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trss);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob) {

                    double base_k = candidate_rank - topk;
                    double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));
                    double delta_k = 0.0;
                    double delta_w = 0.0;
                    double x = 0.0;
                    double y = 0.0;
                    double temp = 0.0;
                    double w1 = 0.0;
                    double w2 = 0.0;
                    double p = 0.0;
                    int cc = 0;

                    refined_k = candidate_rank;
                    refined_w = alpha;
                    Candidatevector v = null;
                    xx = first.normalized_dist;
                    yy = first.normalized_simi;

                    Iterator<Candidatevector> i = candidatevector.iterator();
                    Candidatevector[] vv = new Candidatevector[candidatevector.size()];
                    Candidatevector vvc = null;

                    while (i.hasNext()) {
                        v = i.next();
                        if ((v.dist >= xx) && (v.simi >= yy)) continue;
                        temp = v.simi * xx - yy * v.dist;
                        x = (v.simi - yy) * c / temp;
                        y = (c - xx * x) / yy;

                        w1 = x / (x + y);
                        w2 = 1 - w1;

                        vvc = new Candidatevector(w1, w2);
                        delta_w = Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha));
                        vvc.w_penalty = (1 - penalty_alpha) * delta_w / base_w;

                        vv[cc] = vvc;
                        cc++;
                    }

                    Arrays.sort(vv, 0, cc, new CandidatevectorComparator());

                    for (int cci = 0; cci < cc; cci++) {

                        if (penalty < vv[cci].w_penalty + penalty_alpha * (Math.max(0, vv.length - cc + 1 - topk)) / base_k)
                            break;

                        threshold = (penalty - vv[cci].w_penalty) * base_k / penalty_alpha + topk;

                        Line line = determine_rank(qwords, qpoint, topk, expected_ob, vv[cci].simi, threshold);
                        if (line == null) continue;

                        if (line.initial_rank <= topk) delta_k = 0.0;
                        else delta_k = line.initial_rank - topk;


                        p = penalty_alpha * delta_k / base_k + vv[cci].w_penalty;

                        if (p < penalty) {
                            penalty = p;
                            refined_w = vv[cci].simi;
                            refined_k = line.initial_rank;
                        }
                    }
                    break;
                }

                candidatevector.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }
        }
        System.out.println();
        System.out.println("The refined query is a top-" + refined_k + " query with weight " + refined_w);
        System.out.println("penalty :  " + penalty);

    }

    //new + sort + skip
    public void answeringWhyNot_222(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double penalty_alpha, double c) throws Exception {

        ArrayList<Candidatevector> candidatevector = new ArrayList<Candidatevector>();
        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double xx = 0.0;
        double yy = 0.0;
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;
        int refined_k = 0;
        double refined_w = 0.0;
        double penalty = penalty_alpha;
        double threshold = 0.0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);

                    if (var == null) {

                        continue;
                    }

                    FloatData trs = (FloatData) var;
                    double trss = (double) trs.data / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trss;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trss);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob) {

                    double base_k = candidate_rank - topk;
                    double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));
                    double delta_k = 0.0;
                    double delta_w = 0.0;
                    double x = 0.0;
                    double y = 0.0;
                    double temp = 0.0;
                    double w1 = 0.0;
                    double w2 = 0.0;
                    double p = 0.0;
                    int cc = 0;

                    refined_k = candidate_rank;
                    refined_w = alpha;
                    Candidatevector v = null;
                    xx = first.normalized_dist;
                    yy = first.normalized_simi;

                    Iterator<Candidatevector> i = candidatevector.iterator();
                    Candidatevector[] vv = new Candidatevector[candidatevector.size() + 1];
                    Candidatevector vvc = null;

                    while (i.hasNext()) {
                        v = i.next();
                        if ((v.dist >= xx) && (v.simi >= yy)) continue;
                        temp = v.simi * xx - yy * v.dist;
                        x = (v.simi - yy) * c / temp;
                        y = (c - xx * x) / yy;

                        w1 = x / (x + y);
                        w2 = 1 - w1;

                        vvc = new Candidatevector(w1, w2);
                        delta_w = Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha));
                        vvc.w_penalty = (1 - penalty_alpha) * delta_w / base_w;

                        vv[cc] = vvc;
                        cc++;
                    }

                    vv[cc] = new Candidatevector(1 - alpha, alpha);
                    cc++;
                    Arrays.sort(vv, 0, cc, new CandidatevectorComparator());
                    int index0;
                    for (index0 = 0; index0 < cc; index0++)
                        if (vv[index0].dist == 1 - alpha) break;


                    int rank = candidate_rank;
                    //System.out.print("candidate_rank : " + candidate_rank);
                    for (int j = index0 - 1; j >= 0; j--) {

                        if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(0, rank - j - 1)) / base_k) break;
                        rank--;

                        //early stop
                        //if( penalty < vv[j].w_penalty + penalty_alpha*(Math.max(0,vv.length-cc+1 - topk))/base_k) break;


                        //skip
                        if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k)) continue;

                        Line line = determine_rank(qwords, qpoint, topk, expected_ob, vv[j].simi, 0);

                        rank = line.initial_rank;
                        p = penalty_alpha * Math.max(line.initial_rank - topk, 0) / base_k + vv[j].w_penalty;
                        if (p < penalty) {
                            penalty = p;
                            refined_w = vv[j].simi;
                            refined_k = line.initial_rank;
                        }
                    }


                    rank = candidate_rank;
                    for (int j = index0 + 1; j < cc; j++) {

                        if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(0, rank - cc + j)) / base_k) break;
                        rank--;

                        //early stop
                        //if( penalty < vv[j].w_penalty + penalty_alpha*(Math.max(0,vv.length-cc+1 - topk))/base_k) break;

                        //skip
                        if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k)) continue;

                        Line line = determine_rank(qwords, qpoint, topk, expected_ob, vv[j].simi, 0);

                        rank = line.initial_rank;
                        p = penalty_alpha * Math.max(line.initial_rank - topk, 0) / base_k + vv[j].w_penalty;
                        if (p < penalty) {
                            penalty = p;
                            refined_w = vv[j].simi;
                            refined_k = line.initial_rank;
                        }
                    }


                      /*
                        for(int cci = 0; cci < cc; cci++){

						  if( penalty < vv[cci].w_penalty + penalty_alpha*(Math.max(0,vv.length-cc+1 - topk))/base_k) break;

						  threshold = (penalty - vv[cci].w_penalty)*base_k/penalty_alpha + topk;

						  Line line = determine_rank(qwords, qpoint, topk, expected_ob, vv[cci].simi, threshold);
						  if(line == null) continue;

						  if(line.initial_rank <= topk) delta_k = 0.0;
						  else delta_k = line.initial_rank - topk;


						  p = penalty_alpha*delta_k/base_k+ vv[cci].w_penalty;

						  if(p < penalty) {
							  penalty = p;
							  refined_w = vv[cci].simi;
							  refined_k = line.initial_rank;
						  }
					  }
		  			  */

                    break;
                }

                candidatevector.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }
        }
        System.out.println();
        System.out.println("The refined query is a top-" + refined_k + " query with weight " + refined_w);
        System.out.println("penalty  :  " + penalty);

    }

    //new + sort + skip + test
    public void answeringWhyNot_2222(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double penalty_alpha, double c) throws Exception {

        ArrayList<Candidatevector> candidatevector = new ArrayList<Candidatevector>();
        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double xx = 0.0;
        double yy = 0.0;
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;
        int refined_k = 0;
        double refined_w = 0.0;
        double penalty = penalty_alpha;
        double threshold = 0.0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);

                    if (var == null) {

                        continue;
                    }

                    FloatData trs = (FloatData) var;
                    double trss = (double) trs.data / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trss;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trss);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob) {

                    double base_k = candidate_rank - topk;
                    double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));
                    double delta_k = 0.0;
                    double delta_w = 0.0;
                    double x = 0.0;
                    double y = 0.0;
                    double temp = 0.0;
                    double w1 = 0.0;
                    double w2 = 0.0;
                    double p = 0.0;
                    int cc = 0;

                    refined_k = candidate_rank;
                    refined_w = alpha;
                    Candidatevector v = null;
                    xx = first.normalized_dist;
                    yy = first.normalized_simi;

                    Iterator<Candidatevector> i = candidatevector.iterator();
                    Candidatevector[] vv = new Candidatevector[candidatevector.size() + 1];
                    Candidatevector vvc = null;

                    while (i.hasNext()) {
                        v = i.next();
                        if ((v.dist >= xx) && (v.simi >= yy)) continue;
                        temp = v.simi * xx - yy * v.dist;
                        x = (v.simi - yy) * c / temp;
                        y = (c - xx * x) / yy;

                        w1 = x / (x + y);
                        w2 = 1 - w1;

                        vvc = new Candidatevector(w1, w2);
                        delta_w = Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha));
                        vvc.w_penalty = (1 - penalty_alpha) * delta_w / base_w;

                        vv[cc] = vvc;
                        cc++;
                    }

                    vv[cc] = new Candidatevector(1 - alpha, alpha);
                    cc++;
                    Arrays.sort(vv, 0, cc, new CandidatevectorComparator());
                    int index0;
                    for (index0 = 0; index0 < cc; index0++)
                        if (vv[index0].dist == 1 - alpha) break;

                    LinkedList<Candidatevector> arr = new LinkedList<Candidatevector>();

                    int rank = candidate_rank;
                    if (index0 > 0) {
                        arr = find_higher(qwords, qpoint, topk, expected_ob, vv[0].simi, 0);
                        i = arr.iterator();
                        while (i.hasNext()) {
                            vvc = i.next();
                            if ((vvc.dist >= xx) && (vvc.simi >= yy)) i.remove();
                        }
                        for (int j = index0 - 1; j >= 0; j--) {

                            if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(0, rank - j - 1)) / base_k) break;
                            rank--;

                            //skip
                            if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k))
                                continue;

                            i = arr.iterator();
                            while (i.hasNext()) {
                                vvc = i.next();
                                if (vvc.dist * vv[j].dist + vvc.simi * vv[j].simi > vv[j].dist * first.normalized_dist + vv[j].simi * first.normalized_simi) {
                                    rank++;
                                    i.remove();
                                }
                            }


                            p = penalty_alpha * Math.max(rank - topk, 0) / base_k + vv[j].w_penalty;
                            if (p < penalty) {
                                penalty = p;
                                refined_w = vv[j].simi;
                                refined_k = rank;
                            }
                        }
                    }

                    if (index0 < cc - 1) {
                        arr = find_higher(qwords, qpoint, topk, expected_ob, vv[cc - 1].simi, 0);
                        i = arr.iterator();
                        while (i.hasNext()) {
                            vvc = i.next();
                            if ((vvc.dist >= xx) && (vvc.simi >= yy)) i.remove();
                        }
                        rank = candidate_rank;
                        for (int j = index0 + 1; j < cc; j++) {

                            if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(0, rank - cc + j)) / base_k)
                                break;
                            rank--;

                            //skip
                            if (penalty < vv[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k))
                                continue;

                            i = arr.iterator();
                            while (i.hasNext()) {
                                vvc = i.next();
                                if (vvc.dist * vv[j].dist + vvc.simi * vv[j].simi > vv[j].dist * first.normalized_dist + vv[j].simi * first.normalized_simi) {
                                    rank++;
                                    i.remove();
                                }
                            }

                            p = penalty_alpha * Math.max(rank - topk, 0) / base_k + vv[j].w_penalty;
                            if (p < penalty) {
                                penalty = p;
                                refined_w = vv[j].simi;
                                refined_k = rank;
                            }
                        }
                    }

                    break;
                }

                candidatevector.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }
        }
        System.out.println("The refined query is a top-" + refined_k + " query with weight " + refined_w);
        System.out.println("penalty  :  " + penalty);

    }


    //Algorithm_3 updated new version
    public void answeringWhyNot_3(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatevector> candidatevector = new ArrayList<Candidatevector>(500);
        ArrayList<Cp3> cp3 = new ArrayList<Cp3>(500);
        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double xx = 0.0;
        double yy = 0.0;
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;
        double x0 = 0.0;
        int index0 = 1;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {

            NNEntry first = queue.poll();
            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    if (var == null) {
                        continue;
                    }

                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }

            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob) {

                    double temp = 0.0;
                    double x = 0.0;
                    double y = 0.0;
                    Cp3 p = null;
                    Candidatevector v = null;
                    xx = first.normalized_dist;
                    yy = first.normalized_simi;

                    x0 = c / (xx + yy * alpha / (1 - alpha));
                    y = (c - xx * x0) / yy;
                    cp3.add(new Cp3(x0, y));

                    Iterator<Candidatevector> i = candidatevector.iterator();
                    while (i.hasNext()) {
                        v = i.next();
                        if ((v.dist >= xx) && (v.simi >= yy)) continue;

                        temp = v.simi * xx - yy * v.dist;
                        x = (v.simi - yy) * c / temp;
                        y = (c - xx * x) / yy;

                        p = new Cp3(x, y);
                        index0 = cp3.indexOf(p);
                        if (index0 == -1) cp3.add(p);
                        else {
                            p = cp3.get(index0);
                            p.num++;
                            cp3.set(index0, p);
                        }
                    }
                    cp3.add(new Cp3(0));
                    cp3.add(new Cp3(c / xx));
                    break;
                }
                candidatevector.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }

        }


        //initialization
        //calculate penalty_w for each candidate point
        //find the position of the initial point


        Cp3[] points = new Cp3[cp3.size()];
        int[] degradeup = new int[cp3.size()];
        int[] degradelow = new int[cp3.size()];

        cp3.toArray(points);
        Arrays.sort(points, new Cp3Comparator());


        for (int itt = 0; itt < points.length; itt++)
            System.out.print(points[itt].x + "  ");
        System.out.println();

        //begin help to debug
        int[] levelprune = new int[7];
        System.out.println("number of candiates: " + (points.length - 2));
        //end help to debug

        int numOfCandidate = 0;
        double base_k = (double) candidate_rank - topk;
        double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));

        n = readNode(m_rootID);
        double w1temp = 1.0;
        double w2temp = 1.0;
        for (int i = 0; i < points.length; i++) {
            if (points[i].x == x0) index0 = i;
            else numOfCandidate += points[i].num;

            w1temp = points[i].x / (points[i].x + points[i].y);
            w2temp = 1 - w1temp;
            points[i].penalty_w = Math.sqrt((w1temp + alpha - 1) * (w1temp + alpha - 1) + (w2temp - alpha) * (w2temp - alpha)) / base_w * (1 - penalty_alpha);
        }
        numOfCandidate -= 2;


        //calculate the initial "rank_low" and "rank_up" for each candidate point
        //rank up bound and rank lower bound of each candidate point determined

        points[index0].numFromindex0 = 0;
        points[index0].rank_up = candidate_rank;
        points[index0].rank_low = candidate_rank;
        points[index0].penalty_low = penalty_alpha;
        points[index0].penalty_up = penalty_alpha;

        double penalty_now = penalty_alpha;
        int penalty_now_x = index0;


        for (int i = index0 - 1; i >= 0; i--) {
            points[i].rank_up = candidate_rank - points[i + 1].numFromindex0;
            points[i].rank_low = points[i].rank_up + n.numOfleaf - numOfCandidate;
            points[i].penalty_low = points[i].penalty_w;
            points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;

            points[i].numFromindex0 = points[i + 1].numFromindex0 + points[i].num;
        }

        for (int i = index0 + 1; i < points.length; i++) {
            points[i].rank_up = candidate_rank - points[i - 1].numFromindex0;
            points[i].rank_low = points[i].rank_up + n.numOfleaf - numOfCandidate;
            points[i].penalty_low = points[i].penalty_w;
            points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;

            points[i].numFromindex0 = points[i - 1].numFromindex0 + points[i].num;
        }


        //Start of the prune method~~
        //prune the candidate points step by step

        double upb = c / xx;
        queue.clear();
        n = null;
        Node nn = null;
        int num = 0;
        int kkind = 0;
        nd = new Data(null, null, m_rootID);
        double xtemp_min, xtemp_max;
        int temp;
        int bef, aft;
        int _pruned = 0;


        queue.add(new NNEntry(nd, 0.0, 4, 0, points.length - 1));
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            int i_start = 0;
            int i_end = 0;
            if (first.kind == 0) {
                i_start = first.before + 1;
                i_end = first.after;
            }
            if (first.kind == 1) {
                i_start = Math.max(first.before + 1, index0 + 1);
                i_end = first.after;
            }
            if (first.kind == 2) {
                i_start = first.before + 1;
                i_end = Math.min(index0, first.after);
            }

            temp = 1;
            for (int i = i_start; i < i_end; i++) {
                if (points[i].pruned == 0) {
                    temp = 0;
                    break;
                }
            }

            if (temp == 1) {
                levelprune[first.level]++;
                continue;
            }

            IData fd = (IData) first.m_pEntry;
            n = readNode(fd.getIdentifier());

            iindex.load(n.m_identifier);
            Hashtable trscore = iindex.textRelevancy(qwords);


            for (int i = 0; i < degradeup.length; i++) {
                degradeup[i] = 0;
                degradelow[i] = 0;
            }


            for (int cChild = 0; cChild < n.m_children; cChild++) {
                Object var = trscore.get(n.m_pIdentifier[cChild]);

                if (var == null) continue;

                double trsdata = ((FloatData) var).data;
                double trsdata2 = ((FloatData) var).data2;

                trsdata = trsdata / InvertedIndex.maxTR;
                trsdata2 = trsdata2 / InvertedIndex.maxTR;

                IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);


                double maxdist = 1 - nnc.getMinimumDistance(qpoint, e) / ooo;
                double mindist = 1 - getMaximumDistance(qpoint, e) / ooo;


                if ((mindist >= xx) && (trsdata2 >= yy)) continue;
                if ((maxdist <= xx) && (trsdata <= yy)) continue;

                double score = (1 - alpha) * maxdist + alpha * trsdata;

                if (first.level > 1) {
                    nn = readNode(n.m_pIdentifier[cChild]);
                    num = nn.numOfleaf;
                } else num = 1;


                bef = first.before;
                aft = first.after;


                xtemp_min = 0.0;
                xtemp_max = upb;
                kkind = 0;


                if ((trsdata2 < yy) && (mindist > xx)) {
                    xtemp_max = (trsdata2 - yy) * c / (trsdata2 * xx - yy * mindist);
                    kkind = 1;
                } else if ((trsdata > yy) && (maxdist < xx)) {
                    xtemp_max = (trsdata - yy) * c / (trsdata * xx - yy * maxdist);
                    kkind = 2;
                }


                if ((trsdata2 > yy) && (mindist < xx)) {
                    xtemp_min = (trsdata2 - yy) * c / (trsdata2 * xx - yy * mindist);
                    kkind = 2;
                } else if ((trsdata < yy) && (maxdist > xx)) {
                    xtemp_min = (trsdata - yy) * c / (trsdata * xx - yy * maxdist);
                    kkind = 1;
                }

                if (xtemp_min != 0.0) while (points[bef + 1].x < xtemp_min) bef++;
                if (xtemp_max != upb) while (points[aft - 1].x > xtemp_max) aft--;


                if (kkind == 0) {
                    for (int i = bef + 1; i < aft; i++) degradeup[i] += num;
                } else if (kkind == 1) {
                    if (aft <= index0) continue;

                    for (int i = Math.max(index0, bef) + 1; i < aft; i++) degradeup[i] += num;

                    if (bef < index0) temp = points[bef + 1].numFromindex0;
                    else temp = 0;

                    for (int i = aft; i < first.after; i++) {
                        degradeup[i] += num;
                        degradelow[i] += Math.max(num - temp, 0);
                    }
                } else if (kkind == 2) {
                    if (bef >= index0) continue;
                    for (int i = bef + 1; i < Math.min(index0, aft); i++) degradeup[i] += num;

                    if (aft > index0) temp = points[aft - 1].numFromindex0;
                    else temp = 0;

                    for (int i = first.before + 1; i <= bef; i++) {
                        degradeup[i] += num;
                        degradelow[i] += Math.max(num - temp, 0);
                    }
                }


                if (first.level > 1) {
                    NNEntry e2 = new NNEntry(e, score, n.m_level, bef, aft);
                    e2.kind = kkind;
                    queue.add(e2);
                }

            } // end of the enumeration of the child nodes.


            // update the rank upper bound and lower bound estimated by their father node.
            // prune the candidate points which penalty lower bound is bigger than the known minimal penalty.


            //update
            for (int i = i_start; i < i_end; i++)
                if (points[i].pruned == 0) {
                    if (degradelow[i] > 0) {
                        points[i].rank_up = points[i].rank_up + degradelow[i];
                        points[i].penalty_low = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_up - candidate_rank) / base_k;

                        if (points[i].penalty_low > penalty_now) {
                            points[i].pruned = 1;
                            _pruned++;
                            if (_pruned == points.length - 3) {
                                System.out.println(points[penalty_now_x].rank_low + "   " + points[penalty_now_x].rank_up);
                                System.out.println("finished algorithem III _ updated :  _pruned=" + _pruned);
                            }

                            continue;
                        }
                    }

                    if (degradeup[i] < n.numOfleaf) {
                        points[i].rank_low = points[i].rank_low - n.numOfleaf + degradeup[i];
                        points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;

                        if (points[i].penalty_up < penalty_now) {
                            penalty_now = points[i].penalty_up;
                            penalty_now_x = i;
                        }
                    }
                }


            for (int i = 1; i < points.length; i++)
                if ((points[i].pruned == 0) && (points[i].penalty_low > penalty_now)) {
                    points[i].pruned = 1;
                    _pruned++;
                    if (_pruned == points.length - 3) {
                        //help debug
                        for (int ii = 0; ii < 7; ii++)
                            System.out.println("level : num ----->" + levelprune[ii] + "  :  " + ii);

                        System.out.println(points[penalty_now_x].rank_low + "   " + points[penalty_now_x].rank_up);
                        System.out.println("weight  :   " + points[penalty_now_x].x / (points[penalty_now_x].x + points[penalty_now_x].y));
                        System.out.println("finished algorithem III _ updated :  _pruned=" + _pruned);
                        return;
                        //help debug
                    }
                }


        }


    }

    //modify
    public void answeringWhyNot_3_new(Vector qwords, Point qpoint, int topk, int expected_ob, double alpha, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatevector> candidatevector = new ArrayList<Candidatevector>(500);
        ArrayList<Cp3> cp3 = new ArrayList<Cp3>(500);
        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double xx = 0.0;
        double yy = 0.0;
        double formal_score = -1.0;
        int count = 0;
        int candidate_rank = 0;
        double x0 = 0.0;
        int index0 = 1;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {

            NNEntry first = queue.poll();
            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    if (var == null) {
                        continue;
                    }

                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }

            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                if (first.m_pEntry.getIdentifier() == expected_ob) {

                    double temp = 0.0;
                    double x = 0.0;
                    double y = 0.0;
                    Cp3 p = null;
                    Candidatevector v = null;
                    xx = first.normalized_dist;
                    yy = first.normalized_simi;

                    x0 = c / (xx + yy * alpha / (1 - alpha));
                    y = (c - xx * x0) / yy;
                    cp3.add(new Cp3(x0, y));

                    Iterator<Candidatevector> i = candidatevector.iterator();
                    while (i.hasNext()) {
                        v = i.next();
                        if ((v.dist >= xx) && (v.simi >= yy)) continue;

                        temp = v.simi * xx - yy * v.dist;
                        x = (v.simi - yy) * c / temp;
                        y = (c - xx * x) / yy;

                        p = new Cp3(x, y);
                        index0 = cp3.indexOf(p);
                        if (index0 == -1) cp3.add(p);
                        else {
                            p = cp3.get(index0);
                            p.num++;
                            cp3.set(index0, p);
                        }
                    }
                    cp3.add(new Cp3(0));
                    cp3.add(new Cp3(c / xx));
                    break;
                }
                candidatevector.add(new Candidatevector(first.normalized_dist, first.normalized_simi));
            }

        }


        //initialization
        //calculate penalty_w for each candidate point
        //find the position of the initial point


        Cp3[] points = new Cp3[cp3.size()];
        int[] degradeup = new int[cp3.size()];
        int[] degradelow = new int[cp3.size()];

        cp3.toArray(points);
        Arrays.sort(points, new Cp3Comparator());


        //for(int itt = 0; itt < points.length; itt++)
        //System.out.print(points[itt].x +  "  ");
        //System.out.println();

        //begin help to debug
        int[] levelprune = new int[7];
        //System.out.println("number of candiates: " + (points.length-2));
        //System.out.println("initial_rank : " + candidate_rank);
        //end help to debug

        int numOfCandidate = 0;
        double base_k = (double) candidate_rank - topk;
        double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));

        n = readNode(m_rootID);
        double w1temp = 1.0;
        double w2temp = 1.0;
        for (int i = 0; i < points.length; i++) {
            if (points[i].x == x0) index0 = i;
            else numOfCandidate += points[i].num;

            w1temp = points[i].x / (points[i].x + points[i].y);
            w2temp = 1 - w1temp;
            points[i].penalty_w = Math.sqrt((w1temp + alpha - 1) * (w1temp + alpha - 1) + (w2temp - alpha) * (w2temp - alpha)) / base_w * (1 - penalty_alpha);
        }
        numOfCandidate -= 2;


        //calculate the initial "rank_low" and "rank_up" for each candidate point
        //rank up bound and rank lower bound of each candidate point determined

        points[index0].numFromindex0 = 0;
        points[index0].rank_up = candidate_rank;
        points[index0].rank_low = candidate_rank;
        points[index0].penalty_low = penalty_alpha;
        points[index0].penalty_up = penalty_alpha;
        points[index0].pruned = 1;

        double penalty_now = penalty_alpha;
        int penalty_now_x = index0;


        for (int i = index0 - 1; i >= 0; i--) {
            points[i].numFromindex0 = points[i + 1].numFromindex0 + points[i].num;

            points[i].rank_up = candidate_rank - points[i].numFromindex0;
            points[i].rank_low = n.numOfleaf;  //points[i].rank_up + n.numOfleaf - numOfCandidate;
            points[i].penalty_low = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_up - candidate_rank) / base_k;
            points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;
        }


        for (int i = index0 + 1; i < points.length; i++) {
            points[i].numFromindex0 = points[i - 1].numFromindex0 + points[i].num;

            points[i].rank_up = candidate_rank - points[i].numFromindex0;
            points[i].rank_low = n.numOfleaf; //points[i].rank_up + n.numOfleaf - numOfCandidate;
            points[i].penalty_low = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_up - candidate_rank) / base_k;
            points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;
        }


        //Start of the prune method~~
        //prune the candidate points step by step

        double upb = c / xx;
        queue.clear();
        n = null;
        Node nn = null;
        int num = 0;
        int kkind = 0;
        nd = new Data(null, null, m_rootID);
        double xtemp_min, xtemp_max;
        int temp;
        int bef, aft;
        int _pruned = 0;


        queue.add(new NNEntry(nd, 0.0, 4, 0, points.length - 1));
        while (queue.size() != 0) {

            NNEntry first = (NNEntry) queue.poll();

            if ((first.kind == 1) && (first.after <= index0)) continue;
            if ((first.kind == 2) && (first.before >= index0)) continue;

            int i_start = 0;
            int i_end = 0;
            if (first.kind == 0) {
                i_start = first.before + 1;
                i_end = first.after;
            }
            if (first.kind == 1) {
                i_start = Math.max(first.before + 1, index0 + 1);
                i_end = first.after;
            }
            if (first.kind == 2) {
                i_start = first.before + 1;
                i_end = Math.min(index0, first.after);
            }


            //if node's working part has no promoted point, prune...
            temp = 1;
            for (int i = i_start; i < i_end; i++) {
                if (points[i].pruned == 0) {
                    temp = 0;
                    break;
                }
            }

            levelprune[first.level]++;

            if (temp == 1) {
                continue;
            }
            //prune... ending

            IData fd = (IData) first.m_pEntry;
            n = readNode(fd.getIdentifier());

            iindex.load(n.m_identifier);
            Hashtable trscore = iindex.textRelevancy(qwords);


            for (int i = 0; i < degradeup.length; i++) {
                degradeup[i] = 0;
                degradelow[i] = 0;
            }


            for (int cChild = 0; cChild < n.m_children; cChild++) {
                Object var = trscore.get(n.m_pIdentifier[cChild]);

                if (var == null) continue;

                double trsdata = ((FloatData) var).data;
                double trsdata2 = ((FloatData) var).data2;

                trsdata = trsdata / InvertedIndex.maxTR;
                trsdata2 = trsdata2 / InvertedIndex.maxTR;

                IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);


                double maxdist = 1 - nnc.getMinimumDistance(qpoint, e) / ooo;
                double mindist = 1 - getMaximumDistance(qpoint, e) / ooo;


                if ((mindist >= xx) && (trsdata2 >= yy)) continue;
                if ((maxdist <= xx) && (trsdata <= yy)) continue;

                double score = (1 - alpha) * maxdist + alpha * trsdata;

                if (first.level > 1) {
                    nn = readNode(n.m_pIdentifier[cChild]);
                    num = nn.numOfleaf;
                } else num = 1;


                bef = first.before;
                aft = first.after;


                xtemp_min = 0.0;
                xtemp_max = upb;
                kkind = 0;


                if ((trsdata2 < yy) && (mindist > xx)) {
                    xtemp_max = (trsdata2 - yy) * c / (trsdata2 * xx - yy * mindist);
                    kkind = 1;
                } else if ((trsdata > yy) && (maxdist < xx)) {
                    xtemp_max = (trsdata - yy) * c / (trsdata * xx - yy * maxdist);
                    kkind = 2;
                }


                if ((trsdata2 > yy) && (mindist < xx)) {
                    xtemp_min = (trsdata2 - yy) * c / (trsdata2 * xx - yy * mindist);
                    kkind = 2;
                } else if ((trsdata < yy) && (maxdist > xx)) {
                    xtemp_min = (trsdata - yy) * c / (trsdata * xx - yy * maxdist);
                    kkind = 1;
                }

                if (xtemp_min != 0.0) while (points[bef + 1].x < xtemp_min) bef++;
                if (xtemp_max != upb) while (points[aft - 1].x > xtemp_max) aft--;

                //if(bef > aft) System.out.println("CACACACACACAAAOAAOOAOAOAOAOAOAOAOADDDDDDD!!!!!!!!\n\n\n\n");

                //if(xtemp_min > xtemp_max) System.out.println("CACACACACACAAAOAAOOAOAOAOAOAOAOAOADDDDDDD!!!!!!!!");

                if (kkind == 0) {
                    for (int i = bef + 1; i < aft; i++) degradeup[i] += num;
                } else if (kkind == 1) {
                    if (aft <= index0) continue;

                    for (int i = Math.max(index0, bef) + 1; i < aft; i++) degradeup[i] += num;

                    if (bef < index0) temp = points[bef + 1].numFromindex0;
                    else temp = 0;

                    for (int i = aft; i < first.after; i++) {
                        degradeup[i] += num;
                        degradelow[i] += Math.max(num - temp, 0);
                    }
                } else if (kkind == 2) {
                    if (bef >= index0) continue;
                    for (int i = bef + 1; i < Math.min(index0, aft); i++) degradeup[i] += num;

                    if (aft > index0) temp = points[aft - 1].numFromindex0;
                    else temp = 0;

                    for (int i = first.before + 1; i <= bef; i++) {
                        degradeup[i] += num;
                        degradelow[i] += Math.max(num - temp, 0);
                    }
                }


                if (first.level > 0) {
                    // System.out.println("Inserted!!!!!!!!");
                    NNEntry e2 = new NNEntry(e, score, n.m_level, bef, aft);
                    e2.kind = kkind;
                    queue.add(e2);
                }

            } // end of the enumeration of the child nodes.


            // update the rank upper bound and lower bound estimated by their father node.
            // prune the candidate points which penalty lower bound is bigger than the known minimal penalty.


            //update
            for (int i = i_start; i < i_end; i++)
                if (points[i].pruned == 0) {
                    if (degradelow[i] > 0) {
                        points[i].rank_up = points[i].rank_up + degradelow[i];
                        points[i].penalty_low = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_up - candidate_rank) / base_k;

                        if (points[i].penalty_low > penalty_now) {
                            points[i].pruned = 1;
                            _pruned++;
                            if (_pruned == points.length - 3) {
                                System.out.println(points[penalty_now_x].rank_low + "   " + points[penalty_now_x].rank_up);
                                System.out.println("finished algorithem III _ updated :  _pruned=" + _pruned);
                            }

                            continue;
                        }
                    }

                    if (degradeup[i] < n.numOfleaf) {
                        points[i].rank_low = points[i].rank_low - (n.numOfleaf - degradeup[i]);
                        //  if(points[i].rank_low < n.numOfleaf) System.out.println("FFFFFFFCk");
                        //if(points[i].rank_low < 0) System.out.println("OH shit");
                        //System.out.println(points[i].rank_low +" " + n.numOfleaf+ " " + degradeup[i]);
                        points[i].penalty_up = points[i].penalty_w + penalty_alpha * Math.max(0, points[i].rank_low - candidate_rank) / base_k;

                        if (points[i].penalty_up < penalty_now) {
                            // System.out.println("penalty_now : old & new : " + penalty_now + " & " + points[i].penalty_up);
                            penalty_now = points[i].penalty_up;
                            penalty_now_x = i;
                        }
                    }
                }


            for (int i = 1; i < points.length; i++)
                if ((points[i].pruned == 0) && (points[i].penalty_low > penalty_now)) {
                    points[i].pruned = 1;
                    _pruned++;
                    if (_pruned == points.length - 3) {
                        //help debug
                        // for(int ii = 0; ii < 7; ii++)
                        //	 System.out.println("level : num ----->" +  levelprune[ii]+ "  :  " + ii);

                        //System.out.println(points[penalty_now_x].rank_low+ "   " + points[penalty_now_x].rank_up);
                        // System.out.println("weight  :   " + points[penalty_now_x].x/(points[penalty_now_x].x+points[penalty_now_x].y));
                        double weight = points[penalty_now_x].x / (points[penalty_now_x].x + points[penalty_now_x].y);
                        Line ll = determine_rank(qwords, qpoint, topk, expected_ob, 1 - weight, 0);
                        // System.out.println("refined_k  = " + ll.initial_rank);
                        System.out.println("finished algorithem III _ updated :  _pruned=" + _pruned);
                        return;
                        //help debug
                    }
                }


        }


    }


    public Line[] getWorkingPart(Line[] lines, double c) {
        ArrayList<Line> l_tmp = new ArrayList<Line>();

        int t = 0;
        Line l = lines[t];
        l._start = 0;
        while (true) {
            l._end = c / l.a;
            int k = t;
            for (int i = t + 1; i < lines.length; i++) {
                if (lines[i].a >= l.a) continue;

                double x = (lines[i].b - l.b) * c / (l.a * lines[i].b - l.b * lines[i].a);
                if (x < l._end) {
                    l._end = x;
                    k = i;
                }
            }

            l_tmp.add(l);
            if (k == t) break;
            lines[k]._start = l._end;
            l = lines[k];
        }

        Line[] lin = new Line[l_tmp.size()];
        l_tmp.toArray(lin);
        return lin;
    }


    public Line[] determine_rank_multi(Vector qwords, Point qpoint, int topk, HashSet<Integer> missing, double alpha, double c) throws Exception {
        LinkedList<Line> line_tmp = new LinkedList<Line>();
        Line[] lines = null;

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double formal_score = -1.0;
        int count = 0;
        int i;
        int candidate_rank = 0;
        int numofmissing = missing.size();

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));


        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                i = first.m_pEntry.getIdentifier();
                if (missing.contains(i)) {
                    numofmissing--;

                    //System.out.println(candidate_rank);
                    line_tmp.add(new Line(i, first.normalized_dist, first.normalized_simi));
                    if (numofmissing == 0) {
                        //Begin Sort
                        lines = new Line[line_tmp.size()];
                        line_tmp.toArray(lines);

                        for (int ii = 0; ii < lines.length - 1; ii++)
                            for (int jj = ii + 1; jj < lines.length; jj++)
                                if (lines[ii].b > lines[jj].b) {
                                    Line flag = lines[ii];
                                    lines[ii] = lines[jj];
                                    lines[jj] = flag;
                                }
                        //End Sort
                        //Beign_get working part
                        lines = getWorkingPart(lines, c);
                        //End_get working part
                        break;
                    }
                }
            }
        }

        lines[0].initial_rank = candidate_rank;
        return lines;
    }


    //multiple missing using Algo_1(original)
    public void multimissing_1(Vector qwords, Point qpoint, int topk, double alpha, HashSet<Integer> missing, double penalty_alpha, double c) throws Exception {
        ArrayList<Candidatepoint> candidatepointList = new ArrayList<Candidatepoint>(1000);
        HashSet<Integer> candidatevectorList = new HashSet<Integer>();
        LinkedList<Line> line_tmp = new LinkedList<Line>();
        Line[] lines = determine_rank_multi(qwords, qpoint, topk, missing, alpha, c);

        //System.out.println(lines.length);

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double x = 0.0;
        int rank = 0;
        int i = 0;

        int numofmissing = missing.size();
        Node n = null;
        Data nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));


        //only consider similarity
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                i = first.m_pEntry.getIdentifier();
                if (missing.contains(i)) {
                    numofmissing--;
                    if (numofmissing == 0) break;
                    else continue;
                }

                //Begin_finding candidate points
                candidatevectorList.add(i);
                for (int ii = 0; ii < lines.length; ii++) {
                    if ((first.normalized_dist < lines[ii].a) && (first.normalized_simi > lines[ii].b)) {
                        x = (lines[ii].b - first.normalized_simi) * c / (lines[ii].b * first.normalized_dist - lines[ii].a * first.normalized_simi);
                        if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                        Candidatepoint p = new Candidatepoint(x, ii);
                        i = candidatepointList.indexOf(p);
                        if (i != -1) {
                            p = candidatepointList.get(i);
                            p.in--;
                            candidatepointList.set(i, p);
                        } else {
                            p.in = -1;
                            candidatepointList.add(p);
                        }
                    } else if ((first.normalized_dist > lines[ii].a) && (first.normalized_simi < lines[ii].b)) {
                        x = (lines[ii].b - first.normalized_simi) * c / (lines[ii].b * first.normalized_dist - lines[ii].a * first.normalized_simi);
                        if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                        Candidatepoint p = new Candidatepoint(x, ii);
                        i = candidatepointList.indexOf(p);
                        if (i != -1) {
                            p = candidatepointList.get(i);
                            p.out++;
                            candidatepointList.set(i, p);
                        } else {
                            p.out = 1;
                            candidatepointList.add(p);
                        }
                    }

                }
            }
            //End_finding candidate points
        }


        queue.clear();
        nnc = new NNComparator();
        x = 0.0;
        i = 0;

        n = null;
        nd = new Data(null, null, m_rootID);
        queue.add(new NNEntry(nd, 0.0, 4));
        numofmissing = missing.size();

        //only consider distance
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = 1 - dist;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                i = first.m_pEntry.getIdentifier();
                if (missing.contains(i)) {
                    numofmissing--;
                    if (numofmissing == 0) break;
                    else continue;
                }

                if (candidatevectorList.contains(i)) continue;

                //Begin_finding candidate points
                for (int ii = 0; ii < lines.length; ii++) {
                    if ((first.normalized_dist < lines[ii].a) && (first.normalized_simi > lines[ii].b)) {
                        x = (lines[ii].b - first.normalized_dist) * c / (lines[ii].b * first.normalized_dist - lines[ii].a * first.normalized_simi);
                        if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                        Candidatepoint p = new Candidatepoint(x, ii);
                        i = candidatepointList.indexOf(p);
                        if (i != -1) {
                            p = candidatepointList.get(i);
                            p.in--;
                            candidatepointList.set(i, p);
                        } else {
                            p.in = -1;
                            candidatepointList.add(p);
                        }
                    } else if ((first.normalized_dist > lines[ii].a) && (first.normalized_simi < lines[ii].b)) {
                        x = (lines[ii].b - first.normalized_dist) * c / (lines[ii].b * first.normalized_dist - lines[ii].a * first.normalized_simi);
                        if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                        Candidatepoint p = new Candidatepoint(x, ii);
                        i = candidatepointList.indexOf(p);
                        if (i != -1) {
                            p = candidatepointList.get(i);
                            p.out++;
                            candidatepointList.set(i, p);
                        } else {
                            p.out = 1;
                            candidatepointList.add(p);
                        }
                    }

                }

            }

        }


        Candidatepoint[] points = new Candidatepoint[candidatepointList.size()];
        candidatepointList.toArray(points);
        Arrays.sort(points, new CandidatepointComparator());


        double p = 0.0;
        double y = 0.0;
        double w1 = 0.0;
        double w2 = 0.0;
        double delta_k = 0.0;
        double delta_w = 0.0;
        double base_k = (double) lines[0].initial_rank - topk;
        double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));
        double penalty = penalty_alpha;

        // System.out.println(points.length);

        int refined_k = lines[0].initial_rank;
        rank = refined_k;
        double refined_w = alpha;

        for (int j = 0; j < points.length; j++) {
            rank = rank + points[j].in;

            y = (c - points[j].location_xcoordinate * lines[points[j].loc].a) / lines[points[j].loc].b;
            w1 = points[j].location_xcoordinate / (points[j].location_xcoordinate + y);
            w2 = 1 - w1;
            delta_k = Math.max(0, rank - topk);
            delta_w = Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha));
            p = penalty_alpha * delta_k / base_k + (1 - penalty_alpha) * delta_w / base_w;

            if (p < penalty) {
                penalty = p;
                refined_k = rank;
                refined_w = 1 - w1;
            }

            // System.out.println(w2+" "+penalty+" "+rank+" "+determine_rank(qwords, qpoint, 1,1568434 ,w2, 0).initial_rank);
            rank = rank + points[j].out;

        }
        System.out.println("The refined query is a Top-" + refined_k + " query with weight " + refined_w);
        //System.out.println("penalty:" + penalty);

    }


    //multiple missing using Algo_2(new + sort + skip )
    public void multimissing_2(Vector qwords, Point qpoint, int topk, double alpha, HashSet<Integer> missing, double penalty_alpha, double c) throws Exception {
        LinkedList<Line> line_tmp = new LinkedList<Line>();
        ArrayList<Candidatevector> weightlist = new ArrayList<Candidatevector>(1000);
        ArrayList<Candidatevector> candidatevectorList = new ArrayList<Candidatevector>();
        Candidatevector ctmp = null;
        Line[] lines = null;
        double x0 = 0;

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        double formal_score = -1.0;
        int count = 0;
        int i;
        int candidate_rank = 0;
        int numofmissing = missing.size();

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));
        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);

                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    double trs = ((FloatData) var).data;
                    trs = trs / InvertedIndex.maxTR;

                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs);
                    queue.add(e2);
                }
            } else {
                count++;
                if (formal_score != first.m_minDist) {
                    candidate_rank = count;
                    formal_score = first.m_minDist;
                }

                i = first.m_pEntry.getIdentifier();
                if (missing.contains(i)) {
                    numofmissing--;
                    line_tmp.add(new Line(i, first.normalized_dist, first.normalized_simi));

                    if (numofmissing == 0) {
                        ctmp = new Candidatevector(1 - alpha, alpha);
                        ctmp.loc = i;
                        weightlist.add(ctmp);

                        //Begin Sort
                        lines = new Line[line_tmp.size()];
                        line_tmp.toArray(lines);

                        for (int ii = 0; ii < lines.length - 1; ii++)
                            for (int jj = ii + 1; jj < lines.length; jj++)
                                if (lines[ii].b > lines[jj].b) {
                                    Line flag = lines[ii];
                                    lines[ii] = lines[jj];
                                    lines[jj] = flag;
                                }
                        //End Sort
                        //Beign_get working part
                        lines = getWorkingPart(lines, c);
                        //End_get working part


                        Iterator<Candidatevector> iterator = candidatevectorList.iterator();
                        double x = 0;
                        double y = 0;
                        double w1, w2;
                        double base_k = candidate_rank - topk;
                        double base_w = Math.sqrt(1 + alpha * alpha + (1 - alpha) * (1 - alpha));
                        double delta_k = 0.0;
                        double delta_w = 0.0;
                        int refined_k = candidate_rank;
                        double refined_w = alpha;
                        double penalty = penalty_alpha;


                        Candidatevector cv = null;
                        while (iterator.hasNext()) {
                            ctmp = iterator.next();

                            for (int ii = 0; ii < lines.length; ii++) {
                                double lx0 = c / (lines[ii].a + lines[ii].b * alpha / (1 - alpha));

                                if ((ctmp.dist < lines[ii].a) && (ctmp.simi > lines[ii].b)) {
                                    x = (lines[ii].b - ctmp.simi) * c / (lines[ii].b * ctmp.dist - lines[ii].a * ctmp.simi);
                                    y = (c - lines[ii].a * x) / lines[ii].b;
                                    if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                                    if ((x > lx0)) {
                                        w1 = x / (x + y);
                                        w2 = 1 - w1;
                                        cv = new Candidatevector(w1, w2);
                                        cv.loc = lines[ii].oid;
                                        cv.w_penalty = (1 - penalty_alpha) * Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha)) / base_w;
                                        weightlist.add(cv);
                                    }
                                } else if ((ctmp.dist > lines[ii].a) && (ctmp.simi < lines[ii].b)) {

                                    x = (lines[ii].b - ctmp.simi) * c / (lines[ii].b * ctmp.dist - lines[ii].a * ctmp.simi);
                                    y = (c - lines[ii].a * x) / lines[ii].b;
                                    if (x <= lines[ii]._start || x >= lines[ii]._end) continue;

                                    if (x < lx0) {
                                        w1 = x / (x + y);
                                        w2 = 1 - w1;
                                        cv = new Candidatevector(w1, w2);
                                        cv.loc = lines[ii].oid;
                                        cv.w_penalty = (1 - penalty_alpha) * Math.sqrt((w1 + alpha - 1) * (w1 + alpha - 1) + (w2 - alpha) * (w2 - alpha)) / base_w;
                                        weightlist.add(cv);
                                    }
                                }

                            }
                        }


                        Candidatevector[] weights = new Candidatevector[weightlist.size()];
                        System.out.println(weightlist.size());
                        weightlist.toArray(weights);
                        Arrays.sort(weights, new CandidatevectorComparator());
                        int index0;
                        for (index0 = 0; index0 < weights.length; index0++)
                            if (weights[index0].dist == 1 - alpha) break;


                        int rank = candidate_rank;
                        double p;
                        for (int j = index0 - 1; j >= 0; j--) {

                            if (penalty < weights[j].w_penalty + penalty_alpha * (Math.max(0, rank - j - 1)) / base_k)
                                break;
                            rank--;

                            //early stop
                            //if( penalty < vv[j].w_penalty + penalty_alpha*(Math.max(0,vv.length-cc+1 - topk))/base_k) break;


                            //skip
                            if (penalty < weights[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k))
                                continue;

                            Line line = determine_rank(qwords, qpoint, topk, weights[j].loc, weights[j].simi, 0);

                            rank = line.initial_rank;
                            p = penalty_alpha * Math.max(line.initial_rank - topk, 0) / base_k + weights[j].w_penalty;
                            if (p < penalty) {
                                penalty = p;
                                refined_w = weights[j].simi;
                                refined_k = line.initial_rank;
                            }
                        }


                        rank = candidate_rank;
                        for (int j = index0 + 1; j < weights.length; j++) {

                            if (penalty < weights[j].w_penalty + penalty_alpha * (Math.max(0, rank - weights.length + j)) / base_k)
                                break;
                            rank--;

                            //early stop
                            //if( penalty < vv[j].w_penalty + penalty_alpha*(Math.max(0,vv.length-cc+1 - topk))/base_k) break;

                            //skip
                            if (penalty < weights[j].w_penalty + penalty_alpha * (Math.max(rank - topk, 0) / base_k))
                                continue;

                            Line line = determine_rank(qwords, qpoint, topk, weights[j].loc, weights[j].simi, 0);

                            rank = line.initial_rank;
                            p = penalty_alpha * Math.max(line.initial_rank - topk, 0) / base_k + weights[j].w_penalty;
                            if (p < penalty) {
                                penalty = p;
                                refined_w = weights[j].simi;
                                refined_k = line.initial_rank;
                            }
                        }

                        System.out.println("The refined query is a top-" + refined_k + " query with weight " + refined_w);

                        break;
                    }
                } else {
                    candidatevectorList.add(new Candidatevector(i, first.normalized_dist, first.normalized_simi));
                }


            }
        }
    }


    public void lkt1(Vector qwords, Point qpoint, int topk, double alpha) throws Exception {

        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double knearest = Double.MIN_VALUE;
        ;
        double ooo = Math.sqrt(2);
        int count = 0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (count >= topk && first.m_minDist < knearest) break;

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);


                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    FloatData trs = (FloatData) var;
                    trs.data = trs.data / InvertedIndex.maxTR;
                    trs.data2 = trs.data2 / InvertedIndex.maxTR;

                    //////////////////////////////
                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs.data;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs.data);
                    queue.add(e2);

                }
            } else {
                System.out.println(first.m_pEntry.getIdentifier() + ":" + first.m_minDist);
                count++;
                knearest = first.m_minDist;

            }
        }
    }

    /**
     * 查询流程：
     * 1.从根结点开始，查询倒排索引，找到包含查询关键字的文档id
     * 2.对于所有的子节点，如果包含候选文档id，那么就计算一个得分，存入优先级队列，否则跳过
     * 3.从优先级队列中取出得分最高的结点，如果是内部结点，继续上述过程，否则是外部结点，则其为所求文档
     *
     * @param qwords 查询关键字集合
     * @param qpoint 查询点坐标
     * @param topk   查询返回结果个数
     * @param alpha  空间和文本权重调节系数
     * @return 返回查询结果
     * @throws Exception
     */
    public ArrayList<Integer> Find_AllO_Rank_K(Vector qwords, Point qpoint, int topk, double alpha) throws Exception {

        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double knearest = Double.MIN_VALUE;
        ;
        double ooo = Math.sqrt(2);
        int count = 0;
        int object_id = 0;
        ArrayList<Integer> line = new ArrayList<Integer>();
        // 根结点
        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (count >= topk && first.m_minDist < knearest) break;

            //内部结点
            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);

                // 由查询关键字得到所有候选文档
                Hashtable trscore = iindex.textRelevancy(qwords);


                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {
                        continue;
                    }
                    FloatData trs = (FloatData) var;
                    trs.data = trs.data / InvertedIndex.maxTR;
                    trs.data2 = trs.data2 / InvertedIndex.maxTR;

                    //////////////////////////////
                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs.data;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs.data);
                    queue.add(e2);
                }
            } else {
                //System.out.println(first.m_pEntry.getIdentifier() + ":" + first.m_minDist);
                object_id = first.m_pEntry.getIdentifier();
                count++;//???
                knearest = first.m_minDist;
//                if (count > 10) line.add(object_id);
                line.add(object_id);
                System.err.println(String.format("id %d\tscore %f", first.m_pEntry.getIdentifier(), first.m_minDist));
            }
        }


        if (count == topk) {
            m_stats.m_queryResults = line.size();
            return line;
        }
        return null;
    }

    /**
     * @param qWords             关键字字符形式
     * @param qPoint
     * @param topk
     * @param alpha
     * @param dictionaryFilePath
     * @return
     * @throws Exception
     * @description 使用字符形式的关键字查询
     * @author Pulin Xie
     */
    public ArrayList<Integer> findTopK(String[] qWords, Point qPoint, int topk, double alpha, String dictionaryFilePath) throws Exception {
        Vector<Integer> keysId = Query.findKeyId(qWords, dictionaryFilePath);
        return Find_AllO_Rank_K(keysId, qPoint, topk, alpha);
    }

    public int Find_O_Rank_K(Vector qwords, Point qpoint, int topk, double alpha) throws Exception {

        PriorityQueue<NNEntry> queue = new PriorityQueue<NNEntry>(100, new NNEntryComparator());
        NNComparator nnc = new NNComparator();
        double ooo = Math.sqrt(2);
        int count = 0;
        int object_id = 0;

        Node n = null;
        Data nd = new Data(null, null, m_rootID);

        queue.add(new NNEntry(nd, 0.0, 4));

        while (queue.size() != 0) {
            NNEntry first = (NNEntry) queue.poll();

            if (first.level > 0) {
                IData fd = (IData) first.m_pEntry;
                n = readNode(fd.getIdentifier());

                iindex.load(n.m_identifier);
                Hashtable trscore = iindex.textRelevancy(qwords);


                for (int cChild = 0; cChild < n.m_children; cChild++) {
                    Object var = trscore.get(n.m_pIdentifier[cChild]);
                    //////////////////////////////
                    if (var == null) {

                        continue;
                    }
                    FloatData trs = (FloatData) var;
                    trs.data = trs.data / InvertedIndex.maxTR;
                    trs.data2 = trs.data2 / InvertedIndex.maxTR;

                    //////////////////////////////
                    IEntry e = new Data(n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                    double dist = nnc.getMinimumDistance(qpoint, e) / ooo;
                    double score = (1 - alpha) * (1 - dist) + alpha * trs.data;
                    NNEntry e2 = new NNEntry(e, score, n.m_level, 1 - dist, trs.data);
                    queue.add(e2);

                }
            } else {
                //System.out.println(first.m_pEntry.getIdentifier() + ":" + first.m_minDist);
                object_id = first.m_pEntry.getIdentifier();
                count++;
                if (count == topk) return object_id;
            }
        }

        if (count == topk) return object_id;
        else return -1;
    }

    public long getIO() {
        return m_stats.getReads() + iindex.buffer.getIOs()[0];
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: IRTree docstore tree_file fanout buffersize.");
            System.exit(-1);
        }
        String docfile = args[0];
        String treefile = args[1];
        int fanout = Integer.parseInt(args[2]);
        int buffersize = Integer.parseInt(args[3]);

        BtreeStore docstore = new BtreeStore(docfile, false);

        // Create a disk based storage manager.
        PropertySet ps = new PropertySet();

        ps.setProperty("FileName", treefile);
        // .idx and .dat extensions will be added.

        Integer i = new Integer(4096 * fanout / 100);
        ps.setProperty("PageSize", i);
        // specify the page size. Since the index may also contain user defined data
        // there is no way to know how big a single node may become. The storage manager
        // will use multiple pages per node if needed. Off course this will slow down performance.

        ps.setProperty("BufferSize", buffersize);

        IStorageManager diskfile = new DiskStorageManager(ps);

        IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);
        // applies a main memory random buffer on top of the persistent storage manager
        // (LRU buffer, etc can be created the same way).

        i = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
        ps.setProperty("IndexIdentifier", i);

        IRTree irtree = new IRTree(ps, file, false);

        long start = System.currentTimeMillis();
//		irtree.build("src/regressiontest/test3/dataOfBtree.gz", "irtree", 100, 4096);
        irtree.buildInvertedIndex(docstore);

        long end = System.currentTimeMillis();
        boolean ret = irtree.isIndexValid();
        if (ret == false) System.err.println("Structure is INVALID!");
        irtree.close();

        System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);


    }

    double getMaximumDistance(Point qpoint, IEntry e) {
        IShape s = e.getShape();
        double ans = -1;

        if (s instanceof Point) {
            Point p = (Point) s;
            ans = Math.sqrt((p.m_pCoords[0] - qpoint.m_pCoords[0]) * (p.m_pCoords[0] - qpoint.m_pCoords[0]) +
                    (p.m_pCoords[1] - qpoint.m_pCoords[1]) * (p.m_pCoords[1] - qpoint.m_pCoords[1]));
        } else {
            Region r = (Region) s;
            double temp = 0.0;
            temp = Math.sqrt((r.m_pLow[0] - qpoint.m_pCoords[0]) * (r.m_pLow[0] - qpoint.m_pCoords[0]) +
                    (r.m_pLow[1] - qpoint.m_pCoords[1]) * (r.m_pLow[1] - qpoint.m_pCoords[1]));
            if (temp > ans) ans = temp;
            temp = Math.sqrt((r.m_pHigh[0] - qpoint.m_pCoords[0]) * (r.m_pHigh[0] - qpoint.m_pCoords[0]) +
                    (r.m_pHigh[1] - qpoint.m_pCoords[1]) * (r.m_pHigh[1] - qpoint.m_pCoords[1]));
            if (temp > ans) ans = temp;
            temp = Math.sqrt((r.m_pLow[0] - qpoint.m_pCoords[0]) * (r.m_pLow[0] - qpoint.m_pCoords[0]) +
                    (r.m_pHigh[1] - qpoint.m_pCoords[1]) * (r.m_pHigh[1] - qpoint.m_pCoords[1]));
            if (temp > ans) ans = temp;
            temp = Math.sqrt((r.m_pHigh[0] - qpoint.m_pCoords[0]) * (r.m_pHigh[0] - qpoint.m_pCoords[0]) +
                    (r.m_pLow[1] - qpoint.m_pCoords[1]) * (r.m_pLow[1] - qpoint.m_pCoords[1]));
            if (temp > ans) ans = temp;
        }

        return ans;
    }

    /**
     * 构建IRTree索引
     * @param docsFileName
     * @param btreeName B树的名字，如btree，用来管理文档
     * @param indexFileName 索引的名字，如irtree，索引文件
     * @param fanout
     * @param buffersize
     * @param isCreate
     * @throws Exception
     * @author Pulin Xie
     */
    public static void build(String docsFileName, String btreeName, String indexFileName, int fanout, int buffersize, boolean isCreate) throws Exception {
//        docsFileName = System.getProperty("user.dir") + File.separator + "src" +
//                File.separator + "regressiontest" + File.separator + "test3" + File.separator + docsFileName + ".gz";
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(docsFileName))));
        String propertiesFile = Constants.PROPERTY_DIRECTORY + File.separator +
                docsFileName.substring(docsFileName.indexOf("test") + 5, docsFileName.indexOf('.')) + ".properties";
        docsFileName = Constants.DATA_DIRECTORY + File.separator + docsFileName;
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(docsFileName)));
        /**
         * 1. 用BTree管理docs文件集
         * 2. 利用docs文件集构建RTree索引层
         * 3. 利用BTree的信息构建倒排索引
         */
        //1. BTree管理docs
        BtreeStore bs = BtreeStore.process(docsFileName, btreeName, false);
//        BtreeStore bs = BtreeStore.process(docsFileName, btreeName, isCreate);
        // 2. 构造索引层
        //索引文件管理器，磁盘
        PropertySet ps = new PropertySet();
        // .idx，.dat文件将被创建
        ps.setProperty("FileName", indexFileName);
        Integer pageSize = new Integer(4096 * fanout / 100);
        ps.setProperty("PageSize", pageSize);
        ps.setProperty("BufferSize", buffersize);
        ps.setProperty("Overwrite", isCreate);

        IStorageManager diskfile = new DiskStorageManager(ps);
        IBuffer file = new TreeLRUBuffer(diskfile, buffersize, false);

        ps.setProperty("FillFactor", 0.7);
        ps.setProperty("IndexCapacity", fanout);
        ps.setProperty("LeafCapacity", fanout);
        ps.setProperty("Dimension", 3);

        // 计算最大时间间隔 和 容纳所有对象的最小矩形边长
        double minDistance = 0;
        double maxTime = 0;
        double x0 = 0, y0 = 0, t0 = 0;
        if (isCreate) {
            String line;
            String[] temp;
            float time = 0, x1 = 0, y1 = 0, x2, y2;
            int count = 0;
            double[] f1 = new double[3];
            double[] f2 = new double[3];
            while ((line = reader.readLine()) != null) {
                temp = line.split(",");
                time = Float.parseFloat(temp[1]);
                x1 = Float.parseFloat(temp[2]);
                y1 = Float.parseFloat(temp[3]);
                x2 = Float.parseFloat(temp[4]);
                y2 = Float.parseFloat(temp[5]);

                maxTime = Math.max(maxTime, time);
                minDistance = Math.max(minDistance, x2 - x1);
                minDistance = Math.max(minDistance, y2 - y1);

                x0 = Math.min(x0, x1);
                x0 = Math.min(x0, x2);
                y0 = Math.min(y0, y1);
                y0 = Math.min(y0, y2);
                t0 = Math.min(t0, time);
            }
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(propertiesFile)
            ));
            // 保存计算中间结果到配置文件，后续查询不需要再次计算。
            out.writeDouble(x0);
            out.writeDouble(y0);
            out.writeDouble(t0);
            out.writeDouble(minDistance);
            out.writeDouble(maxTime);
            out.flush();
            out.close();
            reader.close();
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(docsFileName)));
        } else {
            DataInputStream in = new DataInputStream(new BufferedInputStream(
                    new FileInputStream(propertiesFile)));
            x0 = in.readDouble();
            y0 = in.readDouble();
            t0 = in.readDouble();
            minDistance = in.readDouble();
            maxTime = in.readDouble();
            in.close();
        }

        // 如果.idx文件已经建立，该值为m_header的页号
        if (!isCreate)
            ps.setProperty("IndexIdentifier", 1);
        long start = System.currentTimeMillis();
        IRTree irTree = new IRTree(ps, file, isCreate);

        if (isCreate) {
            String line;
            String[] temp;
            int count = 0;
            double[] f1 = new double[3];
            double[] f2 = new double[3];
            while ((line = reader.readLine()) != null) {
                temp = line.split(",");
                int docId = Integer.parseInt(temp[0]);
                float time = Float.parseFloat(temp[1]);
                float x1 = Float.parseFloat(temp[2]);
                float y1 = Float.parseFloat(temp[3]);
                float x2 = Float.parseFloat(temp[4]);
                float y2 = Float.parseFloat(temp[5]);
                // 归一化 和 数据映射
                DataCoordinate coordinates = pretreatment(time, x1, y1, x2, y2, maxTime, minDistance, x0, y0, t0, 0.5);
//                f1[0] = f2[0] = x;
//                f1[1] = f2[1] = y;
                f1[0] = coordinates.x1;
                f2[0] = coordinates.x2;
                f1[1] = coordinates.y1;
                f2[1] = coordinates.y2;
                f1[2] = f2[2] = coordinates.time;
                Region region = new Region(f1, f2);

                byte[] data = new byte[100];

                irTree.insertData(data, region, docId);

                count++;
                if (count % 10000 == 0) System.out.println(count);
            }
            irTree.buildInvertedIndex(bs);

            long end = System.currentTimeMillis();
            System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);
            boolean ret = irTree.isIndexValid();
            if (ret == false) System.err.println("Structure is INVALID!");
        }


//        //Query
//        Vector<Integer> qwords;
//        Random rand = new Random();
//        String[] keys = {
//                "buy", "lol"
//        };
//
//        qwords = Query.findKeyId(keys, Constants.DATA_DIRECTORY + File.separator + "1day/dic1_1.txt");
//
////        qwords.add(2043);
////        qwords.add(2231);
////        qwords.add(711);
////        qwords.add(719);
//        double[] f = new double[3];
//        DataCoordinate coordinates = pretreatment(1523491609241.0, -85.605166, 30.355644, -80.742567, 35.000771, maxTime, minDistance, x0, y0, t0, 0.5);
//        f[0] = (coordinates.x1 + coordinates.x2) / 2;
//        f[1] = (coordinates.y1 + coordinates.y2) / 2;
//        f[2] = coordinates.time;
//        Point qp = new Point(f);
//
//        ArrayList<Integer> list = irTree.Find_AllO_Rank_K(qwords, qp, 10, 0.5);
//        if (list != null && list.size() > 0)
//            System.out.println(list);
//        else System.out.println("Nothing has been found");

        System.err.println(irTree);
        irTree.close();
    }

    /**
     * 预处理的类，该类对数据进行预处理操作。
     * @param time
     * @param x1
     * @param y1
     * @param x2
     * @param y2
     * @param T 给定的时间，默认设置的是给定数据集的最大时间
     * @param d 归一化处理的最大包含所有数据对象的最小矩形的边长
     * @param x0
     * @param y0 给定原点坐标，设置为真实数据中x,y的最小值
     * @param t0 给定时间，设置为给定时间的最小值
     * @param alpha 平滑系数
     * @return
     */
    public static DataCoordinate pretreatment(double time, double x1, double y1, double x2, double y2,
                                              double T, double d, double x0, double y0, double t0, double alpha) {
        assert (alpha >= 0 && alpha <= 1);
        // 归一化
        time = (time - t0) / T;
        x1 = (x1 - x0) / (Math.sqrt(2) * d);
        y1 = (y1 - y0) / (Math.sqrt(2) * d);
        x2 = (x2 - x0) / (Math.sqrt(2) * d);
        y2 = (y2 - y0) / (Math.sqrt(2) * d);

        // 数据映射
        x1 = alpha * Math.sqrt(2) * x1;
        y1 = alpha * Math.sqrt(2) * y1;
        x2 = alpha * Math.sqrt(2) * x2;
        y2 = alpha * Math.sqrt(2) * y2;
        time = (1 - alpha) * Math.sqrt(2) * time;

        return new DataCoordinate(time, x1, y1, x2, y2);
    }
}

/**
 * @description 该类是个辅助类，用来因此返回时空坐标
 * @author Pulin Xie
 */
class DataCoordinate {
    double time;
    double x1, y1, x2, y2;

    public DataCoordinate(double time, double x1, double y1, double x2, double y2) {
        this.time = time;
        this.x1 = x1;
        this.y1 = y1;
        this.x2 = x2;
        this.y2 = y2;
    }
}
