export function generateTotalJobsData() {
  const today = new Date();
  return Array.from({ length: 14 }).map((_, i) => {
    const day = new Date(today);
    day.setDate(today.getDate() - i);
    return {
      date: day.toISOString(),
      jobsCount: i * Math.random(),
    };
  });
}

export function generateFailuresData() {
  const today = new Date();
  return Array.from({ length: 14 }).map((_, i) => {
    const day = new Date(today);
    day.setDate(today.getDate() - i);
    return {
      date: day.toISOString(),
      failures: Math.floor(Math.random() * 50) + 5,
      retries: Math.floor(Math.random() * 100) + 10,
    };
  });
}
