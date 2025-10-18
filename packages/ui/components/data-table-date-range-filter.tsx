"use client";

import * as React from "react";
import { endOfDay, format, startOfDay, subDays, subHours, subMinutes } from "date-fns";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { IconCalendar } from "@tabler/icons-react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface DateRangeFilterProps {
  from?: Date;
  to?: Date;
  onFromChange: (date: Date | undefined) => void;
  onToChange: (date: Date | undefined) => void;
  onReset: () => void;
}

const presets = [
  {
    label: "Last 10 minutes",
    getValue: () => ({
      from: subMinutes(new Date(), 10),
      to: new Date(),
    }),
  },
  {
    label: "Last hour",
    getValue: () => ({
      from: subHours(new Date(), 1),
      to: new Date(),
    }),
  },
  {
    label: "Today",
    getValue: () => ({
      from: startOfDay(new Date()),
      to: endOfDay(new Date()),
    }),
  },
  {
    label: "Yesterday",
    getValue: () => ({
      from: startOfDay(subDays(new Date(), 1)),
      to: endOfDay(subDays(new Date(), 1)),
    }),
  },
  {
    label: "Last 7 days",
    getValue: () => ({
      from: startOfDay(subDays(new Date(), 7)),
      to: endOfDay(new Date()),
    }),
  },
  {
    label: "Last 14 days",
    getValue: () => ({
      from: startOfDay(subDays(new Date(), 14)),
      to: endOfDay(new Date()),
    }),
  },
  {
    label: "Last 30 days",
    getValue: () => ({
      from: startOfDay(subDays(new Date(), 30)),
      to: endOfDay(new Date()),
    }),
  },
];

export function DataTableDateRangeFilter({ from, to, onFromChange, onToChange, onReset }: DateRangeFilterProps) {
  const [isOpen, setIsOpen] = React.useState(false);

  const hasFilter = from || to;

  function formatDateRange() {
    if (from && to) {
      return `${format(from, "MMM d, yyyy")} - ${format(to, "MMM d, yyyy")}`;
    }
    if (from) {
      return `From ${format(from, "MMM d, yyyy")}`;
    }
    if (to) {
      return `Until ${format(to, "MMM d, yyyy")}`;
    }
    return "Enqueued";
  }

  function handlePreset(getValue: () => { from: Date; to: Date }) {
    const { from: presetFrom, to: presetTo } = getValue();
    onFromChange(presetFrom);
    onToChange(presetTo);
  }

  function handleFromChange(e: React.ChangeEvent<HTMLInputElement>) {
    const value = e.target.value;
    if (value) {
      onFromChange(startOfDay(new Date(value)));
    } else {
      onFromChange(undefined);
    }
  }

  function handleToChange(e: React.ChangeEvent<HTMLInputElement>) {
    const value = e.target.value;
    if (value) {
      onToChange(endOfDay(new Date(value)));
    } else {
      onToChange(undefined);
    }
  }

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="h-8 border-dashed">
          <IconCalendar className="mr-2 size-4" />
          {formatDateRange()}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[500px]" align="start">
        <div className="flex gap-4">
          {/* Left column - Presets */}
          <div className="flex-1 space-y-2">
            <h4 className="mb-3 text-sm font-medium uppercase tracking-wide text-muted-foreground">Date Range</h4>
            <div className="space-y-1">
              {presets.map((preset) => (
                <Button
                  key={preset.label}
                  variant="ghost"
                  className="h-10 w-full justify-start px-3 font-normal"
                  onClick={() => handlePreset(preset.getValue)}
                >
                  <span>{preset.label}</span>
                </Button>
              ))}
            </div>
          </div>

          {/* Divider */}
          <div className="w-px bg-border" />

          {/* Right column - Custom Range */}
          <div className="flex-1 space-y-2">
            <h4 className="mb-3 text-sm font-medium uppercase tracking-wide text-muted-foreground">Custom Range</h4>
            <div className="space-y-3">
              <div className="space-y-1.5">
                <Label htmlFor="date-from" className="text-xs text-muted-foreground">
                  Start
                </Label>
                <Input
                  id="date-from"
                  type="datetime-local"
                  value={from ? format(from, "yyyy-MM-dd'T'HH:mm") : ""}
                  onChange={handleFromChange}
                  className="h-9"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="date-to" className="text-xs text-muted-foreground">
                  End
                </Label>
                <Input
                  id="date-to"
                  type="datetime-local"
                  value={to ? format(to, "yyyy-MM-dd'T'HH:mm") : ""}
                  onChange={handleToChange}
                  className="h-9"
                />
              </div>
              <div className="flex items-center justify-end gap-2 pt-2">
                <Button variant="outline" size="sm" onClick={onReset} disabled={!hasFilter}>
                  Clear
                </Button>
                <Button size="sm" onClick={() => setIsOpen(false)}>
                  Apply
                </Button>
              </div>
            </div>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
