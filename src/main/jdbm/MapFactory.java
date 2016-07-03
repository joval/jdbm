/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2014 (C) jOVAL.org. All Rights Reserved.
 *
 * $Id: MapFactory.java,v 1.0 2014/01/21 19:20:18 solind Exp $
 */

package jdbm;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import jdbm.btree.BTree;
import jdbm.helper.IntegerSerializer;
import jdbm.helper.Serializer;
import jdbm.helper.StringComparator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

/**
 * A simplified mechanism for working with JDBM -- the MapFactory makes it easy to create java.util.Map instances
 * whose contents can be serialized to a BTree file store, so they can grow extremely large without consuming too
 * much JVM heap space.
 *
 * @author David A. Solin
 */
public class MapFactory {
    private static final Integer DUMMY_VAL = new Integer(0);
    private static Properties PROPS = new Properties();
    static {
	PROPS.setProperty(RecordManagerOptions.CACHE_TYPE, RecordManagerOptions.NORMAL_CACHE);
	PROPS.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS, "true");
    }

    /**
     * Create a MapFactory. The maps created by the factory will all share a common RecordManager, whose files will be
     * stored in baseDir in files whose names are prefixed with dbkey.
     *
     * @param baseDir The directory in which the JDBM files will be created
     * @param dbkey   The string prefix for JDBM filenames
     */
    public static MapFactory newInstance(File baseDir, String name) throws IOException {
	return new MapFactory(baseDir, name);
    }

    private File baseDir;
    private String dbkey;
    private RecordManager mgr;
    private Collection<MapImpl<?, ?>> maps;

    private MapFactory(File baseDir, String dbkey) throws IOException {
	this.baseDir = baseDir;
	this.dbkey = dbkey;
	String basename = new File(baseDir, dbkey).toString();
	clearFiles();
	mgr = RecordManagerFactory.createRecordManager(basename, PROPS);
	maps = new ArrayList<MapImpl<?, ?>>();
    }

    /**
     * Destroy all the maps created by this factory, and delete the backing store. This method is idempotent.
     */
    public void dispose() throws IOException {
	if (maps != null) {
	    for (MapImpl<?, ?> map : maps) {
		map.dispose();
	    }
	    mgr.commit();
	    maps = null;
	    mgr.close();
	    clearFiles();
	}
    }

    /**
     * Create a new map using the factory's backing store.
     *
     * @param ct The number of cumulative operations that can be performed on the factory's maps before a
     *		 commit is performed on the underlying datastore (higher = faster, lower = less memory).
     */
    public <K, V> Map<K, V> createMap(Comparator<K> keyComp, Serializer keySer, Serializer valSer, int ct) throws IOException {
	MapImpl<K, V> map = new MapImpl<K, V>(keyComp, keySer, valSer, ct);
	maps.add(map);
	return map;
    }

    /**
     * Calls dispose().
     */
    @Override
    protected synchronized void finalize() {
	try {
	    dispose();
	} catch (IOException e) {
	}
    }

    // Internal

    private void clearFiles() throws IOException {
	for (File f : baseDir.listFiles()) {
	    String fname = f.getName();
	    if (fname.equals(dbkey + ".db") || fname.equals(dbkey + ".lg")) {
		if (!f.delete()) {
		    throw new IOException("Failed to delete file: " + f.toString());
		}
	    }
	}
    }

    /**
     * Implementation (partial) of a Map, whose keys and values are stored in a RecordManager.
     */
    public class MapImpl<K, V> implements Map<K, V> {
	private BTree tree;
	private BTree index;
	private int writes, ct;

	MapImpl(Comparator<K> keyComp, Serializer keySer, Serializer valSer, int ct) throws IOException {
	    tree = BTree.createInstance(mgr, keyComp, keySer, valSer);
	    index = BTree.createInstance(mgr, keyComp, keySer, new IntegerSerializer());
	    this.ct = ct;
	}

	void dispose() throws IOException {
	    clear();
	    mgr.delete(tree.getRecid());
	    mgr.delete(index.getRecid());
	}

	// Implement Map

	public boolean containsKey(Object key) {
	    try {
		//
		// Using a separate tree for the index prevents a deserialization infinite loop.
		//
		return index.find(key) != null;
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public boolean containsValue(Object value) {
	    try {
		Tuple t = new Tuple();
		TupleBrowser iter = tree.browse();
		while(iter.getNext(t)) {
		    if (t.getValue().equals(value)) {
			return true;
		    }
		}
		return false;
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public V get(Object key) {
	    try {
		@SuppressWarnings("unchecked")
		V result = (V)tree.find(key);
		return result;
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public boolean isEmpty() {
	    return size() == 0;
	}

	public V put(K key, V value) {
	    try {
		@SuppressWarnings("unchecked")
		V result = (V)tree.insert(key, value, true);
		index.insert(key, DUMMY_VAL, false);
		wrote();
		return result;
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public void putAll(Map<? extends K, ? extends V> m) {
	    try {
		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
		    tree.insert(entry.getKey(), entry.getValue(), true);
		    index.insert(entry.getKey(), DUMMY_VAL, true);
		}
		wrote();
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public V remove(Object key) {
	    try {
		@SuppressWarnings("unchecked")
		V result = (V)tree.remove(key);
		index.remove(key);
		wrote();
		return result;
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public int size() {
	    return index.size();
	}

	public void clear() {
	    try {
		Tuple t = new Tuple();
		TupleBrowser browser = index.browse();
		while(browser.getNext(t)) {
		    index.remove(t.getKey());
		}
		browser = tree.browse();
		while(browser.getNext(t)) {
		    tree.remove(t.getKey());
		}
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	public Set<Map.Entry<K, V>> entrySet() {
	    throw new UnsupportedOperationException();
	}

	public Set<K> keySet() {
	    throw new UnsupportedOperationException();
	}

	public Collection<V> values() {
	    try {
		return new TupleCollection<V>(tree.browse(), tree.size());
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	// Private

	/**
	 * Register performance of a write operation to the cache.  This triggers the occasional commit to disk.
	 */
	private void wrote() throws IOException {
	    if (++writes % ct == 0) {
		synchronized(MapFactory.this) {
		    mgr.commit();
		    for (MapImpl<?, ?> map : maps) {
			map.writes = 0;
		    }
		}
	    }
	}
    }


    static class TupleCollection<E> implements Collection<E> {
	private TupleBrowser tuple;
	private int size;

	TupleCollection(TupleBrowser tuple, int size) {
	    this.tuple = tuple;
	    this.size = size;
	}

	// Implement Collection

	public boolean add(E e) {
	    throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends E> c) {
	    throw new UnsupportedOperationException();
	}

	public void clear() {
	    throw new UnsupportedOperationException();
	}

	public boolean contains(Object o) {
	    throw new UnsupportedOperationException();
	}

	public boolean containsAll(Collection<?> c) {
	    throw new UnsupportedOperationException();
	}

	public boolean isEmpty() {
	    return size == 0;
	}

	public Iterator<E> iterator() {
	    return new TupleIterator<E>(tuple);
	}

	public boolean remove(Object obj) {
	    throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> c) {
	    throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> c) {
	    throw new UnsupportedOperationException();
	}

	public int size() {
	    return size;
	}

	public Object[] toArray() {
	    return toArray(new Object[size]);
	}

	public <T> T[] toArray(T[] a) {
	    if (size != a.length) {
		a = new ArrayList<T>(size).toArray(a);
	    }
	    Iterator<E> iter = iterator();
	    for (int i=0; iter.hasNext(); i++) {
		@SuppressWarnings("unchecked")
		T t = (T)iter.next();
		a[i] = t;
	    }
	    return a;
	}
    }

    static class TupleIterator<E> implements Iterator<E> {
	private TupleBrowser tuple;
	private E next;

	TupleIterator(TupleBrowser tuple) {
	    this.tuple = tuple;
	    next = increment();
	}

	private E increment() {
	    Tuple t = new Tuple();
	    try {
		if (tuple.getNext(t)) {
		    @SuppressWarnings("unchecked")
		    E result = (E)t.getValue();
		    return result;
		} else {
		    return null;
		}
	    } catch (IOException e) {
		throw new RuntimeException(e);
	    }
	}

	// Implement Iterator

	public boolean hasNext() {
	    return next != null;
	}

	public E next() {
	    if (next == null) {
		throw new NoSuchElementException();
	    } else {
		E result = next;
		next = increment();
		return result;
	    }
	}

	public void remove() {
	    throw new UnsupportedOperationException();
	}
    }
}
