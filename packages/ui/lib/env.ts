/* eslint-disable @cspell/spellchecker */
import { z } from "zod";

const connectionSchema = z.object({
  id: z.string(),
  label: z.string(),
  dbUri: z.string(),
});

const envSchema = z.object({
  CONNECTIONS: z.string().transform((val) => {
    return z.array(connectionSchema).parse(JSON.parse(val));
  }),
  NEXTAUTH_SECRET: z.string(),
  NEXTAUTH_URL: z.string(),
  GOOGLE_CLIENT_ID: z.string(),
  GOOGLE_CLIENT_SECRET: z.string(),
  ALLOWED_EMAIL_DOMAIN: z.string(),
  ALLOWED_EMAILS: z
    .string()
    .optional()
    .transform((val) => {
      if (!val) return null;
      return val
        .split(",")
        .map((email) => email.trim())
        .filter((email) => email.length > 0);
    }),
});

export type Env = z.infer<typeof envSchema>;

function validateEnv(): Env {
  return envSchema.parse(process.env);
}

export const env = validateEnv();
