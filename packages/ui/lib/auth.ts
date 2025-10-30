import { env } from "./env";
import GoogleProvider from "next-auth/providers/google";
import { NextAuthOptions } from "next-auth";

export const authOptions: NextAuthOptions = {
  providers: [
    GoogleProvider({
      clientId: env.GOOGLE_CLIENT_ID,
      clientSecret: env.GOOGLE_CLIENT_SECRET,
    }),
  ],
  pages: {
    signIn: "/signin",
    error: "/signupError",
  },
  callbacks: {
    async signIn({ account, profile }) {
      if (account?.provider !== "google") {
        throw new Error("Configuration");
      }

      const email = profile?.email;

      if (!email || !email.endsWith(`@${env.ALLOWED_EMAIL_DOMAIN}`)) {
        throw new Error("WrongEmailDomain");
      }
      if (env.ALLOWED_EMAILS && !env.ALLOWED_EMAILS.includes(email)) {
        throw new Error("NotWhitelisted");
      }
      return true;
    },
    async jwt({ token, account, profile }) {
      if (account && profile) {
        token.email = profile.email;
      }
      return token;
    },
    async session({ session, token }) {
      if (token.email && session.user) {
        session.user.email = token.email as string;
      }
      return session;
    },
  },
};
