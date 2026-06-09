import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { MarkdownMessage } from "@/components/MarkdownMessage";

describe("MarkdownMessage", () => {
  it("renders common Markdown without rendering raw HTML", () => {
    render(
      <MarkdownMessage
        text={[
          "## Heading",
          "",
          "- one",
          "- two",
          "",
          "[docs](https://example.com)",
          "",
          "`inline`",
          "",
          "| A | B |",
          "| - | - |",
          "| 1 | 2 |",
          "",
          "<script>alert('x')</script>",
        ].join("\n")}
      />,
    );

    expect(screen.getByRole("heading", { name: "Heading" })).toBeInTheDocument();
    expect(screen.getByRole("list")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "docs" })).toHaveAttribute(
      "href",
      "https://example.com",
    );
    expect(screen.getByText("inline").tagName).toBe("CODE");
    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(document.querySelector("script")).not.toBeInTheDocument();
    expect(screen.getByText(/<script>alert/)).toBeInTheDocument();
  });
});
