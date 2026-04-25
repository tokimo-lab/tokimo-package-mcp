//! MCP tool-name mangling.
//!
//! Tool names exposed to the LLM must satisfy `^[a-zA-Z0-9_-]{1,64}$`
//! (OpenAI / Anthropic constraint). We wrap every remote tool as
//! `mcp__<server>__<tool>` (CC convention) after sanitising each segment.

pub const MCP_PREFIX: &str = "mcp__";
pub const MAX_TOOL_NAME_LEN: usize = 64;

/// Replace disallowed characters with `_`. Empty input → `"_"`.
pub fn sanitize(raw: &str) -> String {
    if raw.is_empty() {
        return "_".into();
    }
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}

/// Build `mcp__<server>__<tool>` and truncate to fit the 64-char budget.
///
/// When truncation is needed we trim the *tool* segment first (the server
/// segment identifies the source and is more useful for debugging).
pub fn build_mcp_tool_name(server: &str, tool: &str) -> String {
    let server = sanitize(server);
    let tool = sanitize(tool);
    let full = format!("{MCP_PREFIX}{server}__{tool}");
    if full.len() <= MAX_TOOL_NAME_LEN {
        return full;
    }
    // Budget for `tool` segment after `mcp__<server>__`.
    let head = format!("{MCP_PREFIX}{server}__");
    let budget = MAX_TOOL_NAME_LEN.saturating_sub(head.len());
    if budget == 0 {
        // Server segment too long; truncate *that* instead.
        let mut s = full;
        s.truncate(MAX_TOOL_NAME_LEN);
        return s;
    }
    let tool_trimmed: String = tool.chars().take(budget).collect();
    format!("{head}{tool_trimmed}")
}

/// Return `(server, tool)` if `name` is shaped like `mcp__<server>__<tool>`.
pub fn parse_mcp_tool_name(name: &str) -> Option<(&str, &str)> {
    let rest = name.strip_prefix(MCP_PREFIX)?;
    let sep = rest.find("__")?;
    Some((&rest[..sep], &rest[sep + 2..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(build_mcp_tool_name("fs", "read"), "mcp__fs__read");
    }

    #[test]
    fn sanitises_special_chars() {
        assert_eq!(build_mcp_tool_name("my server", "call/it"), "mcp__my_server__call_it");
    }

    #[test]
    fn parses_back() {
        let n = build_mcp_tool_name("fs", "read_file");
        assert_eq!(parse_mcp_tool_name(&n), Some(("fs", "read_file")));
    }

    #[test]
    fn truncates_long() {
        let n = build_mcp_tool_name("shortsrv", &"x".repeat(200));
        assert!(n.len() <= MAX_TOOL_NAME_LEN);
        assert!(n.starts_with("mcp__shortsrv__"));
    }
}
