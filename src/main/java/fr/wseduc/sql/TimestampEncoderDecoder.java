package fr.wseduc.sql;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

public class TimestampEncoderDecoder {

	private static final String baseFormat = "yyyy-MM-dd HH:mm:ss";
	private static final String millisFormat = ".SSSSSS";

	private static final DateTimeParser optional = new DateTimeFormatterBuilder()
			.appendPattern(millisFormat).toParser();
	private static final DateTimeParser optionalTimeZone = new DateTimeFormatterBuilder()
			.appendPattern("Z").toParser();

	private static final DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
			.appendPattern(baseFormat)
			.appendOptional(optional)
			.appendOptional(optionalTimeZone);

	private static final DateTimeFormatter timezonedPrinter = new DateTimeFormatterBuilder()
			.appendPattern(baseFormat + millisFormat + "Z").toFormatter();

	private static final DateTimeFormatter nonTimezonedPrinter = new DateTimeFormatterBuilder()
			.appendPattern(baseFormat + millisFormat).toFormatter();

	private static final DateTimeFormatter formatter = builder.toFormatter();

	public static Object decode(String value) {
		return formatter.parseLocalDateTime(value);
	}

	public static String encode(Object object) {
		if (object instanceof Timestamp || object instanceof Date || object instanceof Calendar) {
			return timezonedPrinter.print(new DateTime(object));
		} else if (object instanceof LocalDateTime) {
			return nonTimezonedPrinter.print((LocalDateTime) object);
		} else if (object instanceof ReadableDateTime) {
			return nonTimezonedPrinter.print((ReadableDateTime) object);
		} else {
			return null;
		}
	}

}
