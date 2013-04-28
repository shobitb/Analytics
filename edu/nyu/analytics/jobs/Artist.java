package edu.nyu.analytics.jobs;

public class Artist {

	public String artist_id;
	public String arist_name;
	
	public Artist(String id) {
		artist_id = id;
	}
	
	public Artist(String id, String name) {
		artist_id = id;
		arist_name = name;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((artist_id == null) ? 0 : artist_id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Artist other = (Artist) obj;
		if (artist_id == null) {
			if (other.artist_id != null)
				return false;
		} else if (!artist_id.equals(other.artist_id))
			return false;
		return true;
	}

}
