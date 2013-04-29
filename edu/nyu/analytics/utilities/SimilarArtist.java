package edu.nyu.analytics.utilities;

public class SimilarArtist implements Comparable<SimilarArtist> {
	private String artist;
	private float value;

	public SimilarArtist(String a, float v) {
		artist = a;
		value = v;
	}

	@Override
	public int compareTo(SimilarArtist a) {
		if (value == a.value) {
			return 0;
		}
		return (this.value > a.value) ? 1 : -1;
	}

	public String getArtist() {
		return artist;
	}

	public double getValue() {
		return value;
	}
}