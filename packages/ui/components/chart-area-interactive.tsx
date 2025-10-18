"use client";

import * as React from "react";
import { Area, AreaChart, CartesianGrid, XAxis } from "recharts";

import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { useIsMobile } from "@/hooks/use-mobile";

export const description = "An interactive area chart";

type TimeRange = "24h" | "7d" | "14d";

interface ChartDataPoint {
  date: string;
  [key: string]: string | number;
}

interface ChartSeries {
  key: string;
  label: string;
  color?: string;
}

interface TimeRangeOption {
  value: TimeRange;
  label: string;
  days: number;
}

interface ChartAreaInteractiveProps {
  title: string;
  description?: string;
  data: ChartDataPoint[];
  series: ChartSeries[];
  timeRangeOptions?: TimeRangeOption[];
  defaultTimeRange?: TimeRange;
}

const DEFAULT_TIME_RANGE_OPTIONS: TimeRangeOption[] = [
  { value: "24h", label: "Last 24 hours", days: 1 },
  { value: "7d", label: "Last 7 days", days: 7 },
  { value: "14d", label: "Last 14 days", days: 14 },
];

export function ChartAreaInteractive({
  title,
  description,
  data,
  series,
  timeRangeOptions = DEFAULT_TIME_RANGE_OPTIONS,
  defaultTimeRange = "7d",
}: ChartAreaInteractiveProps) {
  const isMobile = useIsMobile();
  const [timeRange, setTimeRange] = React.useState<TimeRange>(defaultTimeRange);

  React.useEffect(() => {
    if (isMobile && timeRange === "14d") {
      setTimeRange("7d");
    }
  }, [isMobile, timeRange]);

  // Build chart config dynamically
  const chartConfig = React.useMemo(() => {
    const config: ChartConfig = {};
    series.forEach((s) => {
      config[s.key] = {
        color: s.color || "var(--primary)",
        label: s.label,
      };
    });
    return config;
  }, [series]);

  // Filter data based on time range
  const filteredData = React.useMemo(() => {
    const selectedOption = timeRangeOptions.find((opt) => opt.value === timeRange);
    if (!selectedOption || data.length === 0) return data;

    const now = new Date();
    const startDate = new Date(now);
    startDate.setDate(startDate.getDate() - selectedOption.days);

    return data.filter((item) => {
      const itemDate = new Date(item.date);
      return itemDate >= startDate;
    });
  }, [data, timeRange, timeRangeOptions]);

  // Generate gradient IDs dynamically
  const gradientIds = React.useMemo(() => {
    return series.map((s) => `fill${s.key.charAt(0).toUpperCase() + s.key.slice(1)}`);
  }, [series]);

  const selectedOption = timeRangeOptions.find((opt) => opt.value === timeRange);

  return (
    <Card className="@container/card">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        {description && (
          <CardDescription>
            <span className="hidden @[540px]/card:block">{description}</span>
            <span className="@[540px]/card:hidden">{selectedOption?.label || description}</span>
          </CardDescription>
        )}
        <CardAction>
          <ToggleGroup
            type="single"
            value={timeRange}
            onValueChange={(value) => value && setTimeRange(value as TimeRange)}
            variant="outline"
            className="hidden *:data-[slot=toggle-group-item]:!px-4 @[767px]/card:flex"
          >
            {timeRangeOptions.map((option) => (
              <ToggleGroupItem key={option.value} value={option.value}>
                {option.label}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
          <Select value={timeRange} onValueChange={(value) => setTimeRange(value as TimeRange)}>
            <SelectTrigger
              className="flex w-40 **:data-[slot=select-value]:block **:data-[slot=select-value]:truncate @[767px]/card:hidden"
              size="sm"
              aria-label="Select a time range"
            >
              <SelectValue placeholder={selectedOption?.label} />
            </SelectTrigger>
            <SelectContent className="rounded-xl">
              {timeRangeOptions.map((option) => (
                <SelectItem key={option.value} value={option.value} className="rounded-lg">
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </CardAction>
      </CardHeader>
      <CardContent className="px-2 pt-4 sm:px-6 sm:pt-6">
        <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full">
          <AreaChart data={filteredData}>
            <defs>
              {series.map((s, index) => (
                <linearGradient key={s.key} id={gradientIds[index]} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={`var(--color-${s.key})`} stopOpacity={index === 0 ? 1.0 : 0.8} />
                  <stop offset="95%" stopColor={`var(--color-${s.key})`} stopOpacity={0.1} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              minTickGap={32}
              tickFormatter={(value) => {
                const date = new Date(value);
                if (timeRange === "24h") {
                  return date.toLocaleTimeString("en-US", {
                    hour: "2-digit",
                    minute: "2-digit",
                  });
                }
                return date.toLocaleDateString("en-US", {
                  day: "numeric",
                  month: "short",
                });
              }}
            />
            <ChartTooltip
              cursor={false}
              content={
                <ChartTooltipContent
                  labelFormatter={(value) => {
                    const date = new Date(value);
                    if (timeRange === "24h") {
                      return date.toLocaleString("en-US", {
                        day: "numeric",
                        hour: "2-digit",
                        minute: "2-digit",
                        month: "short",
                      });
                    }
                    return date.toLocaleDateString("en-US", {
                      day: "numeric",
                      month: "short",
                    });
                  }}
                  indicator="dot"
                />
              }
            />
            {series.map((s, index) => (
              <Area
                key={s.key}
                dataKey={s.key}
                type="natural"
                fill={`url(#${gradientIds[index]})`}
                stroke={`var(--color-${s.key})`}
                stackId="a"
              />
            ))}
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
