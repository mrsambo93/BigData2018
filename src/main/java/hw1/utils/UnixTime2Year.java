package hw1.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UnixTime2Year {
	
	public static int unixTime2Year(long unixSeconds){
		Date date = new Date(unixSeconds*1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		return Integer.parseInt(sdf.format(date));
	}

}
