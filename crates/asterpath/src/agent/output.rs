//! Structured output modes (ADR-0034): which requests carry the output tool,
//! and the config rules that make tool mode consistent.

use asterpath_protocol::{
    ModelRequest, OutputFormat, OutputMode, ProviderCapabilities, RequestConfig, ToolChoice,
    ToolDescriptor, ToolName,
};

/// The fixed result that closes every output tool call.
pub(crate) const DELIVERED: &str = "result delivered";

/// The output tool this config puts in a request for a model with
/// `capabilities`, or `None` when the request uses the native constraint.
pub(crate) fn tool<'a>(
    request: &'a RequestConfig,
    capabilities: &ProviderCapabilities,
) -> Option<&'a OutputFormat> {
    let format = request.output_format.as_ref()?;
    let tool = match format.mode {
        // Config checks keep the host's tool choice compatible.
        OutputMode::Tool => true,
        OutputMode::Auto => {
            !capabilities.structured_output
                && capabilities.forced_tool_choice
                && allows(&request.tool_choice, format)
        }
        _ => false,
    };
    tool.then_some(format)
}

/// Swap the native constraint for the output tool and a forced tool call.
/// The tool joins the baseline too, so the tool-set fingerprint covers it.
pub(crate) fn apply(request: &mut ModelRequest, format: &OutputFormat) {
    let descriptor = ToolDescriptor::new(
        ToolName::new(&format.name),
        "Deliver the final result: call this tool with the result as its arguments.".to_owned(),
        format.schema.clone(),
    );
    request.tools.push(descriptor.clone());
    request.baseline_tools.push(descriptor);
    if !matches!(request.tool_choice, ToolChoice::Tool(_)) {
        request.tool_choice = ToolChoice::Required;
    }
    request.output_format = None;
}

/// A tool choice that still lets the model call the output tool.
fn allows(choice: &ToolChoice, format: &OutputFormat) -> bool {
    match choice {
        ToolChoice::None => false,
        ToolChoice::Tool(name) => name.as_str() == format.name,
        _ => true,
    }
}

/// Tool mode needs a tool choice that allows the output tool; tool and auto
/// mode need a valid tool name that `registered` does not already use.
pub(crate) fn check(
    request: &RequestConfig,
    registered: impl Fn(&ToolName) -> bool,
) -> Result<(), String> {
    let Some(format) = &request.output_format else {
        return Ok(());
    };
    if format.mode == OutputMode::Native {
        return Ok(());
    }
    if format.mode == OutputMode::Tool && !allows(&request.tool_choice, format) {
        return Err(format!(
            "output tool {} cannot be called under tool choice {:?}",
            format.name, request.tool_choice
        ));
    }
    asterpath_protocol::validate_tool_name(&format.name)
        .map_err(|error| format!("invalid output tool name {}: {error}", format.name))?;
    if registered(&ToolName::new(&format.name)) {
        return Err(format!(
            "output tool {} has the name of a registered tool",
            format.name
        ));
    }
    Ok(())
}
