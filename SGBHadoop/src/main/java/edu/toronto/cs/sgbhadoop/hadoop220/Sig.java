package edu.toronto.cs.sgbhadoop.hadoop220;

public class Sig implements Comparable<Sig> {
	final public String p;
	final public String idko;
	final int hashcode;
	final public String mashed;

	public Sig(String p, String idko) {
		this.p = p;
		this.idko = idko;
		mashed = p + "." + idko;
		hashcode = mashed.hashCode();
	}

	@Override
	public int compareTo(Sig o) {
		return mashed.compareTo(o.mashed);
		//		int result = p.compareTo(o.p);
		//		if (result == 0) {
		//			result = idko.compareTo(o.idko);
		//		}
		//		return result;
	}

	@Override
	public int hashCode() {
		return hashcode;
		//		return (p+"."+idko).hashCode();
		//		return p.hashCode() * 13 + idko.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return mashed.equals(((Sig) obj).mashed);
		//		Sig o = (Sig) obj;
		//		return p.equals(o.p) && idko.equals(o.idko);
	}

	@Override
	public String toString() {
		return "sig:" + mashed;
		//		return "sig: " + p + ", " + idko;
	}
}