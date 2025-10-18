import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(num: number): string {
  if (num >= 1000) {
    return (num / 1000).toFixed(1).replace(/\.0$/, "") + "K";
  }
  return num.toString();
}

export function formatDuration(ms: number) {
  if (ms < 1000) {
    return `${ms.toFixed(0)}ms`;
  } else if (ms < 60_000) {
    return `${(ms / 1000).toFixed(0)}s`;
  } else if (ms < 3_600_000) {
    return `${(ms / 60_000).toFixed(0)}min`;
  } else {
    return `${(ms / 3_600_000).toFixed(0)}h`;
  }
}

export function parseCronExpression(cron: string): string {
  if (!cron) return "Invalid cron";

  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return cron; // Return original if not standard 5-part cron

  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

  // Handle common patterns
  if (cron === "* * * * *") return "Every minute";
  if (cron === "0 * * * *") return "Every hour";
  if (cron === "0 0 * * *") return "Daily at midnight";
  if (cron === "0 0 * * 0") return "Weekly on Sunday at midnight";
  if (cron === "0 0 1 * *") return "Monthly on the 1st at midnight";

  // Build description
  const parts_desc: string[] = [];

  // Minute
  if (minute === "*") {
    parts_desc.push("every minute");
  } else if (minute.startsWith("*/")) {
    parts_desc.push(`every ${minute.slice(2)} minutes`);
  } else {
    parts_desc.push(`at minute ${minute}`);
  }

  // Hour
  if (hour !== "*") {
    if (hour.startsWith("*/")) {
      parts_desc.push(`every ${hour.slice(2)} hours`);
    } else {
      parts_desc.push(`at ${hour}:00`);
    }
  }

  // Day of month
  if (dayOfMonth !== "*") {
    if (dayOfMonth.startsWith("*/")) {
      parts_desc.push(`every ${dayOfMonth.slice(2)} days`);
    } else {
      parts_desc.push(`on day ${dayOfMonth}`);
    }
  }

  // Month
  if (month !== "*") {
    const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const monthNum = parseInt(month, 10);
    if (monthNum >= 1 && monthNum <= 12) {
      parts_desc.push(`in ${monthNames[monthNum - 1]}`);
    } else {
      parts_desc.push(`in month ${month}`);
    }
  }

  // Day of week
  if (dayOfWeek !== "*") {
    const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const dayNum = parseInt(dayOfWeek, 10);
    if (dayNum >= 0 && dayNum <= 6) {
      parts_desc.push(`on ${dayNames[dayNum]}`);
    } else {
      parts_desc.push(`on day ${dayOfWeek}`);
    }
  }

  return parts_desc.join(" ");
}
