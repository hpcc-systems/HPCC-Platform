package framework.utility;

import framework.config.Config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeUtils {
    private static final Pattern TIME_PATTERN = Pattern.compile(
            "\\s*(\\d+)\\s+days\\s+(\\d+):(\\d+):(\\d+).(\\d+)|" +
                    "(\\d+):(\\d+):(\\d+).(\\d+)|" +
                    "(\\d+):(\\d+).(\\d+)|" +
                    "(\\d+).(\\d+)\\s*");

    private static final long MILLISECONDS_IN_A_SECOND = 1000;
    private static final long MILLISECONDS_IN_A_MINUTE = 60 * MILLISECONDS_IN_A_SECOND;
    private static final long MILLISECONDS_IN_AN_HOUR = 60 * MILLISECONDS_IN_A_MINUTE;
    private static final long MILLISECONDS_IN_A_DAY = 24 * MILLISECONDS_IN_AN_HOUR;

    public static long convertToMilliseconds(String time) {
        Matcher matcher = TIME_PATTERN.matcher(time);
        if (matcher.matches()) {
            int days = 0;
            int hours = 0;
            int minutes = 0;
            int seconds = 0;
            int milliSeconds = 0;

            try {
                if (matcher.group(1) != null) { // Matches d days h:m:s.s
                    days = Integer.parseInt(matcher.group(1));
                    hours = Integer.parseInt(matcher.group(2));
                    minutes = Integer.parseInt(matcher.group(3));
                    seconds = Integer.parseInt(matcher.group(4));
                    milliSeconds = Integer.parseInt(matcher.group(5));
                } else if (matcher.group(6) != null) { // Matches h:m:s.s
                    hours = Integer.parseInt(matcher.group(6));
                    minutes = Integer.parseInt(matcher.group(7));
                    seconds = Integer.parseInt(matcher.group(8));
                    milliSeconds = Integer.parseInt(matcher.group(9));
                } else if (matcher.group(10) != null) { // Matches m:s.s
                    minutes = Integer.parseInt(matcher.group(10));
                    seconds = Integer.parseInt(matcher.group(11));
                    milliSeconds = Integer.parseInt(matcher.group(12));
                } else if (matcher.group(13) != null) { // Matches s.s
                    seconds = Integer.parseInt(matcher.group(13));
                    milliSeconds = Integer.parseInt(matcher.group(14));
                }
            } catch (NumberFormatException e) {
                return Config.MALFORMED_TIME_STRING;
            }

            return days * MILLISECONDS_IN_A_DAY +
                    hours * MILLISECONDS_IN_AN_HOUR +
                    minutes * MILLISECONDS_IN_A_MINUTE +
                    seconds * MILLISECONDS_IN_A_SECOND +
                    milliSeconds;

        } else {
            return Config.MALFORMED_TIME_STRING;
        }
    }
}
