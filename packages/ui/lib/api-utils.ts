import { NextResponse } from "next/server";

export function handleApiError(error: unknown) {
  const message = error instanceof Error ? error.message : "Unknown error";
  return NextResponse.json({ error: "Internal server error", details: message }, { status: 500 });
}

export function withErrorHandling(handler: (req: Request) => Promise<Response>) {
  return async (req: Request) => {
    try {
      return await handler(req);
    } catch (error) {
      return handleApiError(error);
    }
  };
}
