FROM node:20-alpine AS builder
RUN corepack enable
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/ui/package.json ./packages/ui/
COPY packages/ui ./packages/ui
RUN pnpm install --frozen-lockfile
RUN pnpm --filter mysql-queue-ui run build


FROM node:20-alpine AS runner
WORKDIR /app
COPY --from=builder /app/packages/ui/.next/standalone ./.next/standalone
COPY --from=builder /app/packages/ui/public ./.next/standalone/packages/ui/public
COPY --from=builder /app/packages/ui/.next/static ./.next/standalone/packages/ui/.next/static
EXPOSE 3000
CMD ["node", ".next/standalone/packages/ui/server.js"]
