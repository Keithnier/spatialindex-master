// Spatial Index Library
//
// Copyright (C) 2002  Navel Ltd.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Contact information:
//  Mailing address:
//    Marios Hadjieleftheriou
//    University of California, Riverside
//    Department of Computer Science
//    Surge Building, Room 310
//    Riverside, CA 92521
//
//  Email:
//    marioh@cs.ucr.edu

package spatialindex.storagemanager;

import java.util.ArrayList;
import java.util.Stack;

public class MemoryStorageManager implements IStorageManager {

    //用来保存
	private ArrayList m_buffer = new ArrayList();
	//用于存储内容为空的数组(ArrayList中的数组)下标，以防止ArrayList过度扩张
	private Stack m_emptyPages = new Stack();

    /**
     * 直接从List中获取数据
     * @param id
     * @return
     */
	public byte[] loadByteArray(final int id) {
		Entry e = null;
		try {
			e = (Entry) m_buffer.get(id);
		}
		catch (IndexOutOfBoundsException ex) {
			throw new InvalidPageException(id);
		}
		byte[] ret = new byte[e.m_pData.length];
		System.arraycopy(e.m_pData, 0, ret, 0, e.m_pData.length);
		return ret;
	}

    /**
     * 如果emptyPage里有空的，则从中获取空的下标，存储，没有则直接存到ArrayList中
     * @param id
     * @param data
     * @return
     */
	public int storeByteArray(final int id, final byte[] data) {
		int ret = id;
		Entry e = new Entry(data);
		if (id == NewPage) {
			if (m_emptyPages.empty()) {
				m_buffer.add(e);
				ret = m_buffer.size() - 1;
			}
			else {
				ret = ((Integer) m_emptyPages.pop()).intValue();
				m_buffer.set(ret, e);
			}
		} else {
			if (id < 0 || id >= m_buffer.size()) throw new InvalidPageException(id);
			m_buffer.set(id, e);
		}

		return ret;
	}

    /**
     * 从List中删除节点，并将节点Id存入emptyPage的Stack中
     * @param id
     */
	public void deleteByteArray(final int id) {
		Entry e = null;
		try {
			e = (Entry) m_buffer.get(id);
		} catch (IndexOutOfBoundsException ex) {
			throw new InvalidPageException(id);
		}

		m_buffer.set(id, null);
		m_emptyPages.push(new Integer(id));
	}

    public void flush() {
    }

	class Entry {
		byte[] m_pData;

		Entry(final byte[] d) {
			m_pData = new byte[d.length];
			System.arraycopy(d, 0, m_pData, 0, d.length);
		}
	} // Entry
}
