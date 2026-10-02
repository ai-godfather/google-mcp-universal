"""
PageSpeed Fixes Registry & Patch Engine
Defines all known performance fixes for Shopify templates.
Each fix has: metadata, applicability check, and patch generator.
"""

import os
import re
import difflib
import logging

logger = logging.getLogger("pagespeed_fixes")

# All ACTIVE_SHOPS templates are new_tpl (Tailwind)
TEMPLATE_TYPE = "new_tpl"

FIXES_REGISTRY = {
    "PSI-001": {
        "name": "Defer IP fetch in helperscripts.liquid",
        "severity": "CRITICAL",
        "files": ["snippets/helperscripts.liquid"],
        "estimated_savings_ms": 400,
        "metrics_impact": {"fcp_ms": 400, "lcp_ms": 200, "tbt_ms": 100, "cls": 0},
        "description": "Wraps the synchronous fetch() to ipify.org/cloudflare in requestIdleCallback to avoid blocking page render",
        "generate_patches": None,
    },
    "PSI-002": {
        "name": "Lazy-load popupExit JS and defer initialization",
        "severity": "CRITICAL",
        "files": ["snippets/popupExit.liquid"],
        "estimated_savings_ms": 350,
        "metrics_impact": {"fcp_ms": 300, "lcp_ms": 100, "tbt_ms": 200, "cls": 0.1},
        "description": "Wraps popupExit initialization in load event + setTimeout to defer heavy JS execution",
        "generate_patches": None,
    },
    "PSI-003": {
        "name": "Move phone button CSS to lazy-loaded external style or defer",
        "severity": "HIGH",
        "files": ["layout/theme.liquid"],
        "estimated_savings_ms": 200,
        "metrics_impact": {"fcp_ms": 200, "lcp_ms": 0, "tbt_ms": 0, "cls": 0},
        "description": "The ~8KB inline phone button CSS with vendor-prefixed animations is loaded on product pages. Use media=print trick to defer.",
        "generate_patches": None,
    },
    "PSI-010": {
        "name": "Add defer to jQuery 3.6",
        "severity": "CRITICAL",
        "files": ["layout/theme.liquid"],
        "estimated_savings_ms": 300,
        "metrics_impact": {"fcp_ms": 300, "lcp_ms": 0, "tbt_ms": 100, "cls": 0},
        "description": "jquery360.js is loaded without defer, blocking render. Add defer attribute.",
        "generate_patches": None,
    },
    "PSI-011": {
        "name": "Add defer to Splide.js",
        "severity": "HIGH",
        "files": ["layout/theme.liquid"],
        "estimated_savings_ms": 150,
        "metrics_impact": {"fcp_ms": 100, "lcp_ms": 0, "tbt_ms": 50, "cls": 0},
        "description": "splide.js loaded without defer on ALL pages. Add defer attribute.",
        "generate_patches": None,
    },
    "PSI-012": {
        "name": "Remove console.log from production",
        "severity": "LOW",
        "files": ["layout/theme.liquid", "snippets/helperscripts.liquid", "snippets/popupExit.liquid"],
        "estimated_savings_ms": 10,
        "metrics_impact": {"fcp_ms": 0, "lcp_ms": 0, "tbt_ms": 10, "cls": 0},
        "description": "Remove all console.log statements from production code",
        "generate_patches": None,
    },
}


def detect_template_type(theme_liquid_content):
    """
    Detect template type from theme.liquid content.
    All ACTIVE_SHOPS templates are new_tpl (Tailwind-based).
    """
    if "tailwindcss.css" in theme_liquid_content:
        return "new_tpl"
    return "unknown"


def generate_unified_diff(old_content, new_content, filename):
    """Generate a unified diff string between old and new content."""
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    diff = difflib.unified_diff(old_lines, new_lines, fromfile=filename, tofile=filename)
    return "".join(diff)


def get_all_repo_names(repos_path):
    """List all repo directories in repos_path (ACTIVE_SHOPS directory).
    Also tries repos_path/ACTIVE_SHOPS as fallback."""
    search_path = repos_path
    if not os.path.isdir(search_path):
        return []
    # Check if there are theme repos directly in repos_path
    candidates = [
        d for d in os.listdir(search_path)
        if os.path.isdir(os.path.join(search_path, d))
        and os.path.exists(os.path.join(search_path, d, "layout", "theme.liquid"))
    ]
    if candidates:
        return sorted(candidates)
    # Fallback: try repos_path/ACTIVE_SHOPS
    active_shops = os.path.join(repos_path, "ACTIVE_SHOPS")
    if os.path.isdir(active_shops):
        return sorted([
            d for d in os.listdir(active_shops)
            if os.path.isdir(os.path.join(active_shops, d))
            and os.path.exists(os.path.join(active_shops, d, "layout", "theme.liquid"))
        ])
    return []


def _generate_psi001_patches(repo_path):
    """
    PSI-001: Defer IP fetch in helperscripts.liquid
    Wrap fetch() call in requestIdleCallback to avoid blocking page render.
    """
    patches = []
    helper_file = os.path.join(repo_path, "snippets", "helperscripts.liquid")

    if not os.path.isfile(helper_file):
        return patches

    with open(helper_file, 'r', encoding='utf-8', errors='ignore') as f:
        original_content = f.read()

    new_content = original_content

    # Pattern 1: ipify.org fetch
    ipify_pattern = (
        r"(\s+)fetch\('https://api\.ipify\.org\?format=json'\)"
        r"(\s+)\.then\(response => response\.json\(\)\)"
        r"(\s+)\.then\(data => \{[^}]*setCookie\('userIp'[^}]*\}[^}]*\)"
        r"(\s+)\.catch\(error => \{[^}]*\}\);"
    )

    # Simpler pattern: just wrap the entire fetch block
    fetch_block_pattern = (
        r"(let userIpAd = '';\n)"
        r"([ \t]*)fetch\('https://api\.ipify\.org\?format=json'\)"
        r"([\s\S]*?)"
        r"(\.catch\(error => \{[\s\S]*?\}\);)"
    )

    match = re.search(fetch_block_pattern, new_content)
    if match:
        indent = match.group(2)
        fetch_start = match.group(1)
        fetch_body = match.group(3)
        fetch_end = match.group(4)

        # Build the wrapped version
        wrapped_version = (
            f"{fetch_start}"
            f"{indent}if ('requestIdleCallback' in window) {{\n"
            f"{indent}    requestIdleCallback(function() {{\n"
            f"{indent}        fetch('https://api.ipify.org?format=json')"
            f"{fetch_body}"
            f"{fetch_end}\n"
            f"{indent}    }});\n"
            f"{indent}}} else {{\n"
            f"{indent}    setTimeout(function() {{\n"
            f"{indent}        fetch('https://api.ipify.org?format=json')"
            f"{fetch_body}"
            f"{fetch_end}\n"
            f"{indent}    }}, 3000);\n"
            f"{indent}}}"
        )

        new_content = new_content[:match.start()] + wrapped_version + new_content[match.end():]

    if new_content != original_content:
        diff = generate_unified_diff(original_content, new_content, "snippets/helperscripts.liquid")
        patches.append({
            "file": "snippets/helperscripts.liquid",
            "old_content": original_content,
            "new_content": new_content,
            "diff": diff,
        })

    return patches


def _generate_psi002_patches(repo_path):
    """
    PSI-002: Lazy-load popupExit JS and defer initialization.
    All popupExit.liquid files use DOMContentLoaded to init their main logic.
    We change DOMContentLoaded → 'load' and wrap the handler in setTimeout(3000)
    so the heavy popup JS doesn't execute until well after page load.
    Also defers popupStyle.css loading using media="print" trick.
    """
    patches = []
    popup_file = os.path.join(repo_path, "snippets", "popupExit.liquid")

    if not os.path.isfile(popup_file):
        return patches

    with open(popup_file, 'r', encoding='utf-8', errors='ignore') as f:
        original_content = f.read()

    new_content = original_content

    # Fix 1: Replace DOMContentLoaded with load + setTimeout wrapper
    # Pattern: window.addEventListener('DOMContentLoaded', (event) => {
    # or:      window.addEventListener('DOMContentLoaded', function() {
    dcl_patterns = [
        # Arrow function variant
        (
            r"window\.addEventListener\('DOMContentLoaded',\s*\(event\)\s*=>\s*\{",
            "window.addEventListener('load', function() { setTimeout(function() {"
        ),
        # Regular function variant
        (
            r"window\.addEventListener\('DOMContentLoaded',\s*function\s*\(\)\s*\{",
            "window.addEventListener('load', function() { setTimeout(function() {"
        ),
        # Double-quote variant
        (
            r'window\.addEventListener\("DOMContentLoaded",\s*\(event\)\s*=>\s*\{',
            "window.addEventListener('load', function() { setTimeout(function() {"
        ),
    ]

    dcl_replaced = False
    for pattern, replacement in dcl_patterns:
        if re.search(pattern, new_content):
            new_content = re.sub(pattern, replacement, new_content, count=1)
            dcl_replaced = True
            break

    # If we replaced DOMContentLoaded, we need to add the closing for setTimeout
    # Find the matching closing }); for the DOMContentLoaded event listener
    # The last </script> in the main block should have });
    # We add }, 3000); before the final });
    if dcl_replaced:
        # Find the LAST occurrence of });  before the first </script>
        # The pattern is: ...code... }); </script>
        # We want: ...code... }, 3000); }); </script>
        # Simpler approach: find the last "})" before "</script>" and add setTimeout closing
        last_script_end = new_content.rfind("</script>")
        if last_script_end > 0:
            # Search backwards from last </script> for the closing });
            before_end = new_content[:last_script_end].rstrip()
            # Check if it ends with }); — this is the DOMContentLoaded/load listener close
            if before_end.endswith("});"):
                # Insert setTimeout closing before the final });
                insert_pos = before_end.rfind("});")
                new_content = (
                    new_content[:insert_pos] +
                    "}, 3000); // PSI-002: defer popup init by 3s\n    " +
                    new_content[insert_pos:]
                )

    # Fix 2: Defer popupStyle.css loading using media="print" trick
    css_pattern = r"\{\{\s*'popupStyle\.css'\s*\|\s*asset_url\s*\|\s*stylesheet_tag\s*\}\}"
    css_replacement = (
        '<link rel="stylesheet" href="{{ \'popupStyle.css\' | asset_url }}" media="print" onload="this.media=\'all\'">'
    )
    new_content = re.sub(css_pattern, css_replacement, new_content)

    if new_content != original_content:
        diff = generate_unified_diff(original_content, new_content, "snippets/popupExit.liquid")
        patches.append({
            "file": "snippets/popupExit.liquid",
            "old_content": original_content,
            "new_content": new_content,
            "diff": diff,
        })

    return patches


def _generate_psi003_patches(repo_path):
    """
    PSI-003: Move phone button CSS to lazy-loaded using media="print" trick.
    Find the large <style> block with .phoneBtnContainer and keyframes,
    and apply the print media trick for deferred loading.
    """
    patches = []
    theme_file = os.path.join(repo_path, "layout", "theme.liquid")

    if not os.path.isfile(theme_file):
        return patches

    with open(theme_file, 'r', encoding='utf-8', errors='ignore') as f:
        original_content = f.read()

    new_content = original_content

    # Find the style block with phoneBtnContainer
    # Pattern: <style> ... .phoneBtnContainer ... </style> (inside {% if template contains 'product' %})
    style_pattern = (
        r"(\s+)<style>\n"
        r"([ \t]*\.phoneBtnContainer\{[^\}]*\}[\s\S]*?)"
        r"([ \t]*#trigger-popup[^\}]*\}[^\}]*\})"
        r"([\s\S]*?)"
        r"([ \t]*</style>)"
    )

    # More permissive pattern
    style_pattern_simple = (
        r"(<style>)\n"
        r"([ \t]*\.phoneBtnContainer.*?)</style>"
    )

    match = re.search(r"<style>\s*\.phoneBtnContainer", new_content)
    if match:
        # Find the full style block
        start_idx = match.start()
        end_idx = new_content.find("</style>", start_idx)

        if end_idx != -1:
            end_idx += len("</style>")
            style_block = new_content[start_idx:end_idx]

            # Replace with media="print" version
            modified_style = style_block.replace(
                "<style>",
                '<style id="phone-btn-css" media="print">'
            ).replace(
                "</style>",
                "</style>\n    <script>document.getElementById('phone-btn-css').media='all';</script>"
            )

            new_content = new_content[:start_idx] + modified_style + new_content[end_idx:]

    if new_content != original_content:
        diff = generate_unified_diff(original_content, new_content, "layout/theme.liquid")
        patches.append({
            "file": "layout/theme.liquid",
            "old_content": original_content,
            "new_content": new_content,
            "diff": diff,
        })

    return patches


def _generate_psi010_patches(repo_path):
    """
    PSI-010: Add defer to jQuery 3.6.
    Find: <script src="{{ 'jquery360.js' | asset_url }}"></script>
    Replace with: <script src="{{ 'jquery360.js' | asset_url }}" defer></script>
    """
    patches = []
    theme_file = os.path.join(repo_path, "layout", "theme.liquid")

    if not os.path.isfile(theme_file):
        return patches

    with open(theme_file, 'r', encoding='utf-8', errors='ignore') as f:
        original_content = f.read()

    new_content = original_content

    # Pattern: <script src="{{ 'jquery360.js' | asset_url }}"></script>
    jquery_pattern = r"<script src=\"\{\{ 'jquery360\.js' \| asset_url \}\}\"></script>"

    if re.search(jquery_pattern, new_content):
        new_content = re.sub(
            jquery_pattern,
            '<script src="{{ \'jquery360.js\' | asset_url }}" defer></script>',
            new_content
        )

    if new_content != original_content:
        diff = generate_unified_diff(original_content, new_content, "layout/theme.liquid")
        patches.append({
            "file": "layout/theme.liquid",
            "old_content": original_content,
            "new_content": new_content,
            "diff": diff,
        })

    return patches


def _generate_psi011_patches(repo_path):
    """
    PSI-011: Add defer to Splide.js.
    Find: {{ 'splide.js' | asset_url | script_tag }}
    Replace with: <script src="{{ 'splide.js' | asset_url }}" defer></script>
    """
    patches = []
    theme_file = os.path.join(repo_path, "layout", "theme.liquid")

    if not os.path.isfile(theme_file):
        return patches

    with open(theme_file, 'r', encoding='utf-8', errors='ignore') as f:
        original_content = f.read()

    new_content = original_content

    # Pattern: {{ 'splide.js' | asset_url | script_tag }}
    splide_pattern = r"\{\{ 'splide\.js' \| asset_url \| script_tag \}\}"

    if re.search(splide_pattern, new_content):
        new_content = re.sub(
            splide_pattern,
            '<script src="{{ \'splide.js\' | asset_url }}" defer></script>',
            new_content
        )

    if new_content != original_content:
        diff = generate_unified_diff(original_content, new_content, "layout/theme.liquid")
        patches.append({
            "file": "layout/theme.liquid",
            "old_content": original_content,
            "new_content": new_content,
            "diff": diff,
        })

    return patches


def _generate_psi012_patches(repo_path):
    """
    PSI-012: Remove console.log statements from production code.
    Searches all target files and removes console.log(...) lines/blocks.
    """
    patches = []
    target_files = [
        "layout/theme.liquid",
        "snippets/helperscripts.liquid",
        "snippets/popupExit.liquid",
    ]

    for target_file in target_files:
        file_path = os.path.join(repo_path, target_file)

        if not os.path.isfile(file_path):
            continue

        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            original_content = f.read()

        new_content = original_content

        # Pattern 1: Simple single-line console.log
        # console.log(...);
        new_content = re.sub(
            r"^\s*console\.log\([^)]*\);\s*$",
            "",
            new_content,
            flags=re.MULTILINE
        )

        # Pattern 2: console.log with string messages
        new_content = re.sub(
            r"\s*console\.log\(['\"][^'\"]*['\"]\);?\n?",
            "",
            new_content
        )

        # Pattern 3: console.log with variables
        new_content = re.sub(
            r"\s*console\.log\([^)]+\);\n?",
            "",
            new_content
        )

        # Clean up resulting multiple blank lines
        new_content = re.sub(r"\n\n\n+", "\n\n", new_content)

        if new_content != original_content:
            diff = generate_unified_diff(original_content, new_content, target_file)
            patches.append({
                "file": target_file,
                "old_content": original_content,
                "new_content": new_content,
                "diff": diff,
            })

    return patches


def apply_fix(repo_path, fix_id, dry_run=True):
    """
    Apply a specific fix to a repository.

    Args:
        repo_path: Path to the repository root
        fix_id: Fix ID (e.g., "PSI-001")
        dry_run: If True, only return patches without writing to files

    Returns:
        List of patch dicts with file, old_content, new_content, diff
    """
    if fix_id not in FIXES_REGISTRY:
        logger.error(f"Unknown fix ID: {fix_id}")
        return []

    fix = FIXES_REGISTRY[fix_id]
    generate_patches_fn = fix.get("generate_patches")

    if generate_patches_fn is None:
        logger.error(f"Fix {fix_id} does not have a patch generator")
        return []

    patches = generate_patches_fn(repo_path)

    if not dry_run:
        for patch in patches:
            file_path = os.path.join(repo_path, patch["file"])
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(patch["new_content"])
            logger.info(f"Applied fix {fix_id} to {patch['file']}")

    return patches


# Register all patch generators
FIXES_REGISTRY["PSI-001"]["generate_patches"] = _generate_psi001_patches
FIXES_REGISTRY["PSI-002"]["generate_patches"] = _generate_psi002_patches
FIXES_REGISTRY["PSI-003"]["generate_patches"] = _generate_psi003_patches
FIXES_REGISTRY["PSI-010"]["generate_patches"] = _generate_psi010_patches
FIXES_REGISTRY["PSI-011"]["generate_patches"] = _generate_psi011_patches
FIXES_REGISTRY["PSI-012"]["generate_patches"] = _generate_psi012_patches


# Utility: Apply all critical fixes to a repository
def apply_all_critical_fixes(repo_path, dry_run=True):
    """Apply all CRITICAL severity fixes to a repository."""
    all_patches = []

    for fix_id, fix_metadata in FIXES_REGISTRY.items():
        if fix_metadata["severity"] == "CRITICAL":
            patches = apply_fix(repo_path, fix_id, dry_run=dry_run)
            all_patches.extend(patches)

    return all_patches


# Utility: Get summary of all available fixes
def get_fixes_summary():
    """Return a summary of all available fixes."""
    summary = []

    for fix_id, metadata in sorted(FIXES_REGISTRY.items()):
        summary.append({
            "id": fix_id,
            "name": metadata["name"],
            "severity": metadata["severity"],
            "estimated_savings_ms": metadata["estimated_savings_ms"],
            "files": metadata["files"],
            "description": metadata["description"],
            "metrics_impact": metadata["metrics_impact"],
        })

    return summary


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Print all available fixes
    print("Available PageSpeed Fixes:")
    print("=" * 80)
    for fix in get_fixes_summary():
        print(f"\n{fix['id']}: {fix['name']}")
        print(f"  Severity: {fix['severity']}")
        print(f"  Files: {', '.join(fix['files'])}")
        print(f"  Est. Savings: {fix['estimated_savings_ms']}ms")
        print(f"  Description: {fix['description']}")
        print(f"  Metrics: {fix['metrics_impact']}")
