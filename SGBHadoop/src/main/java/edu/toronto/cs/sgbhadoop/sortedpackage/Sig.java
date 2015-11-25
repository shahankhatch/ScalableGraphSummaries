package edu.toronto.cs.sgbhadoop.sortedpackage;

public class Sig implements Comparable<Sig> {
	public String p;
	public String idko;

	public void setIdko(String idko) {
		this.idko = idko;
	}

	public void setP(String p) {
		this.p = p;
	}

	public Sig(String p, String idko) {
		this.p = p;
		this.idko = idko;
	}

	@Override
	public int compareTo(Sig o) {
		int result = p.compareTo(o.p);
		if (result == 0) {
			result = idko.compareTo(o.idko);
		}
		return result;
	}

	@Override
	public int hashCode() {
		return p.hashCode() * 13 + idko.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		Sig o = (Sig) obj;
		return p.equals(o.p) && idko.equals(o.idko);
	}

	@Override
	public String toString() {
		return "sig: " + p + ", " + idko;
	}
}