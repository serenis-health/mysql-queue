import { useEffect, useState } from "react";
import { formatDistanceToNowStrict } from "date-fns";

interface LiveTimeProps {
  date: Date;
  alwaysUpdate?: boolean;
}

export function LiveTime({ date }: LiveTimeProps) {
  const [timeAgo, setTimeAgo] = useState(formatDistanceToNowStrict(date, { addSuffix: true }));

  useEffect(() => {
    const minutesFromNowAndDate = Math.abs(new Date().getTime() - date.getTime()) / 1000 / 60;

    if (minutesFromNowAndDate > 2) return;

    const interval = setInterval(() => {
      setTimeAgo(formatDistanceToNowStrict(date, { addSuffix: true }));
    }, 1000);

    return () => clearInterval(interval);
  }, [date]);

  return <span>{timeAgo}</span>;
}
