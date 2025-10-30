"use client";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import { useSearchParams } from "next/navigation";

const errorMessages: Record<string, { title: string; description: string }> = {
  WrongEmailDomain: {
    title: "Wrong Email Domain",
    description:
      "You must use an email from the allowed domain to access this application. Please sign in with an authorized email address.",
  },
  NotWhitelisted: {
    title: "Email Not Whitelisted",
    description:
      "Your email address is not on the approved list for this application. Please contact your administrator to request access.",
  },
  Configuration: {
    title: "Configuration Error",
    description: "There is a problem with the server configuration. Please contact support.",
  },
  Verification: {
    title: "Verification Failed",
    description: "The verification token has expired or has already been used.",
  },
  Default: {
    title: "Authentication Error",
    description: "An error occurred during authentication. Please try again.",
  },
};

export default function AuthErrorPage() {
  const searchParams = useSearchParams();
  const error = searchParams.get("error") || "Default";
  const { title, description } = errorMessages[error] || errorMessages.Default;

  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-1">
          <div className="flex items-center gap-2">
            <AlertCircle className="h-6 w-6 text-destructive" />
            <CardTitle className="text-2xl font-bold">{title}</CardTitle>
          </div>
          <CardDescription>{description}</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <Button asChild className="w-full">
            <Link href="/signin">Back to Sign In</Link>
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
