"use client";

import { IconPlayerPause, IconPlayerPlay, IconX } from "@tabler/icons-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Button } from "@/components/ui/button";
import { DataTableDateRangeFilter } from "./data-table-date-range-filter";
import { DataTableFacetedFilter } from "./data-table-faceted-filter";
import { formatNumber } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import { Table } from "@tanstack/react-table";

interface DataTableToolbarProps<TData> {
  table: Table<TData>;
  searchColumn?: string;
  searchPlaceholder?: string;
  globalFilterValue?: string;
  onGlobalFilterChange?: (value: string) => void;
  dateFrom?: Date;
  dateTo?: Date;
  onDateFromChange?: (date: Date | undefined) => void;
  onDateToChange?: (date: Date | undefined) => void;
  totalCount?: number;
  currentCount?: number;
  isLoading?: boolean;
  entityName?: string;
  autoReloadEnabled?: boolean;
  onAutoReloadEnabledChange?: (enabled: boolean) => void;
  autoReloadInterval?: 5 | 15 | 30;
  onAutoReloadIntervalChange?: (interval: 5 | 15 | 30) => void;
  filters?: {
    column: string;
    title: string;
    options: {
      label: string;
      value: string;
      icon?: React.ComponentType<{ className?: string }>;
      count?: number;
    }[];
    value?: string[];
    onChange?: (values: string[]) => void;
  }[];
}

export function DataTableToolbar<TData>({
  table,
  searchColumn,
  searchPlaceholder,
  globalFilterValue,
  onGlobalFilterChange,
  dateFrom,
  dateTo,
  onDateFromChange,
  onDateToChange,
  totalCount,
  currentCount,
  isLoading,
  entityName = "job",
  autoReloadEnabled,
  onAutoReloadEnabledChange,
  autoReloadInterval,
  onAutoReloadIntervalChange,
  filters,
}: DataTableToolbarProps<TData>) {
  const hasDateFilter = dateFrom || dateTo;
  const hasControlledFilters = filters?.some((f) => f.value && f.value.length > 0) || false;
  const isFiltered =
    table.getState().columnFilters.length > 0 ||
    (globalFilterValue && globalFilterValue.length > 0) ||
    hasDateFilter ||
    hasControlledFilters;

  function handleReset() {
    table.resetColumnFilters();
    onGlobalFilterChange?.("");
    onDateFromChange?.(undefined);
    onDateToChange?.(undefined);
    filters?.forEach((filter) => {
      filter.onChange?.([]);
    });
  }

  function handleDateReset() {
    onDateFromChange?.(undefined);
    onDateToChange?.(undefined);
  }

  return (
    <div className="flex items-center justify-between">
      <div className="flex flex-1 items-center space-x-2">
        {searchColumn && !onGlobalFilterChange && (
          <Input
            placeholder={searchPlaceholder || "Search..."}
            value={(table.getColumn(searchColumn)?.getFilterValue() as string) ?? ""}
            onChange={(event) => table.getColumn(searchColumn)?.setFilterValue(event.target.value)}
            className="h-8 w-[150px] lg:w-[250px]"
          />
        )}
        {onGlobalFilterChange && (
          <Input
            placeholder={searchPlaceholder || "Search..."}
            value={globalFilterValue ?? ""}
            onChange={(event) => onGlobalFilterChange(event.target.value)}
            className="h-8 w-[150px] lg:w-[250px]"
          />
        )}
        {onDateFromChange && onDateToChange && (
          <DataTableDateRangeFilter
            from={dateFrom}
            to={dateTo}
            onFromChange={onDateFromChange}
            onToChange={onDateToChange}
            onReset={handleDateReset}
          />
        )}
        {filters?.map((filter) => {
          const column = table.getColumn(filter.column);
          return column ? (
            <DataTableFacetedFilter
              key={filter.column}
              column={column}
              title={filter.title}
              options={filter.options}
              value={filter.value}
              onChange={filter.onChange}
            />
          ) : null;
        })}
        {isFiltered && (
          <Button variant="ghost" onClick={handleReset} className="h-8 px-2 lg:px-3">
            Reset
            <IconX className="ml-2 size-4" />
          </Button>
        )}
      </div>
      {totalCount !== undefined && currentCount !== undefined && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          {isLoading && (
            <svg className="size-4 animate-spin" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
          )}
          <span>
            {formatNumber(currentCount)} of {formatNumber(totalCount)} {totalCount === 1 ? entityName : `${entityName}s`}
          </span>
          {onAutoReloadEnabledChange && onAutoReloadIntervalChange && (
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant={autoReloadEnabled ? "default" : "outline"}
                  size="sm"
                  className="h-7 gap-1"
                  onClick={(e) => {
                    if (autoReloadEnabled) {
                      e.preventDefault();
                      onAutoReloadEnabledChange(false);
                    }
                  }}
                >
                  {autoReloadEnabled ? <IconPlayerPause className="size-3" /> : <IconPlayerPlay className="size-3" />}
                  <span className="text-xs">{autoReloadInterval}s</span>
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-40 p-2" align="end">
                <div className="flex flex-col gap-1">
                  <div className="mb-1 text-xs font-medium">Reload interval</div>
                  {[5, 15, 30].map((interval) => (
                    <Button
                      key={interval}
                      variant={autoReloadInterval === interval ? "default" : "ghost"}
                      size="sm"
                      className="h-7 w-full justify-start text-xs"
                      onClick={() => {
                        onAutoReloadIntervalChange(interval as 5 | 15 | 30);
                        if (!autoReloadEnabled) {
                          onAutoReloadEnabledChange(true);
                        }
                      }}
                    >
                      {interval} seconds
                    </Button>
                  ))}
                </div>
              </PopoverContent>
            </Popover>
          )}
        </div>
      )}
    </div>
  );
}
