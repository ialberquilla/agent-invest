import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { cn } from "@/lib/utils";

type MarkdownMessageProps = {
  text: string;
  className?: string;
};

export function MarkdownMessage({ text, className }: MarkdownMessageProps) {
  return (
    <div className={cn("break-words", className)}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
        p: ({ className, ...props }) => (
          <p className={cn("my-3 first:mt-0 last:mb-0", className)} {...props} />
        ),
        ul: ({ className, ...props }) => (
          <ul className={cn("my-3 list-disc space-y-1 pl-6", className)} {...props} />
        ),
        ol: ({ className, ...props }) => (
          <ol className={cn("my-3 list-decimal space-y-1 pl-6", className)} {...props} />
        ),
        li: ({ className, ...props }) => (
          <li className={cn("pl-1", className)} {...props} />
        ),
        a: ({ className, ...props }) => (
          <a
            className={cn("font-medium text-primary underline underline-offset-4", className)}
            target="_blank"
            rel="noreferrer"
            {...props}
          />
        ),
        code: ({ className, ...props }) => (
          <code
            className={cn(
              "rounded bg-muted px-1.5 py-0.5 font-mono text-[0.9em]",
              className,
            )}
            {...props}
          />
        ),
        pre: ({ className, ...props }) => (
          <pre
            className={cn(
              "my-4 overflow-x-auto rounded-xl bg-muted p-4 font-mono text-sm leading-6",
              className,
            )}
            {...props}
          />
        ),
        blockquote: ({ className, ...props }) => (
          <blockquote
            className={cn(
              "my-4 border-l-4 border-border pl-4 text-muted-foreground",
              className,
            )}
            {...props}
          />
        ),
        table: ({ className, ...props }) => (
          <div className="my-4 overflow-x-auto">
            <table
              className={cn("w-full border-collapse text-sm", className)}
              {...props}
            />
          </div>
        ),
        th: ({ className, ...props }) => (
          <th
            className={cn("border border-border bg-muted px-3 py-2 text-left font-semibold", className)}
            {...props}
          />
        ),
        td: ({ className, ...props }) => (
          <td className={cn("border border-border px-3 py-2", className)} {...props} />
        ),
        }}
      >
        {text}
      </ReactMarkdown>
    </div>
  );
}
