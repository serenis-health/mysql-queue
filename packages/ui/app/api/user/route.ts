import { authOptions } from "@/lib/auth";
import { getServerSession } from "next-auth";
import { NextResponse } from "next/server";
import { User } from "@/types/user";

export async function GET(_req: Request) {
  const session = await getServerSession(authOptions);
  const user: User = {
    avatar: session!.user?.image || undefined,
    email: session!.user?.email || "",
    name: session?.user?.name || "",
  };
  return NextResponse.json(user);
}
