#!/usr/bin/env python3
"""Render deterministic cover and headboard composites for Sail Rust Book."""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


BOOK_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BOOK_ROOT.parent
FIRSTPAIR_ROOT = Path.home() / "src" / "firstpair"
ASSET_DIR = BOOK_ROOT / "assets" / "cover"

COVER_BG = ASSET_DIR / "sail-rust-book-cover-background.png"
HEADBOARD_BG = ASSET_DIR / "sail-rust-book-headboard-background.png"
MASK_LOGO = FIRSTPAIR_ROOT / "logo" / "firstpair-publisher-mask.png"

COVER_OUT = ASSET_DIR / "sail-rust-book-cover.png"
HEADBOARD_OUT = ASSET_DIR / "sail-rust-book-headboard.png"

TITLE = "Sail Rust Book"
SUBTITLE = "The Rust, Arrow, and DataFusion Guide"
AUTHOR = "Alexy Khrabrov ∈ LakeSail Team"

WHITE = (246, 252, 255, 255)
CYAN = (126, 222, 255, 255)
AMBER = (255, 204, 118, 255)
NAVY = (4, 16, 30, 230)


def font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size)


SERIF = "/System/Library/Fonts/Supplemental/Georgia.ttf"
SERIF_BOLD = "/System/Library/Fonts/Supplemental/Georgia Bold.ttf"
SANS = "/System/Library/Fonts/Supplemental/Arial.ttf"
SANS_BOLD = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
SYMBOL_TEXT = "/System/Library/Fonts/Supplemental/Arial Unicode.ttf"


def cover_resize(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    src_w, src_h = image.size
    dst_w, dst_h = size
    scale = max(dst_w / src_w, dst_h / src_h)
    resized = image.resize((round(src_w * scale), round(src_h * scale)), Image.Resampling.LANCZOS)
    left = (resized.width - dst_w) // 2
    top = (resized.height - dst_h) // 2
    return resized.crop((left, top, left + dst_w, top + dst_h))


def gradient_overlay(size: tuple[int, int], top: int, bottom: int, color: tuple[int, int, int]) -> Image.Image:
    width, height = size
    overlay = Image.new("RGBA", size, (0, 0, 0, 0))
    pixels = overlay.load()
    for y in range(height):
        alpha = int(top + (bottom - top) * (y / max(1, height - 1)))
        for x in range(width):
            pixels[x, y] = (*color, alpha)
    return overlay


def side_gradient(size: tuple[int, int], left_alpha: int, right_alpha: int, color: tuple[int, int, int]) -> Image.Image:
    width, height = size
    overlay = Image.new("RGBA", size, (0, 0, 0, 0))
    pixels = overlay.load()
    for x in range(width):
        alpha = int(left_alpha + (right_alpha - left_alpha) * (x / max(1, width - 1)))
        for y in range(height):
            pixels[x, y] = (*color, max(0, min(255, alpha)))
    return overlay


def mask_as_tint(width: int, color: tuple[int, int, int, int], opacity: float = 0.85) -> Image.Image:
    logo = Image.open(MASK_LOGO).convert("L")
    logo = logo.resize((width, round(width * logo.height / logo.width)), Image.Resampling.LANCZOS)
    alpha = logo.point(lambda v: int(max(0, v - 8) * opacity))
    out = Image.new("RGBA", logo.size, color)
    out.putalpha(alpha)
    return out


def paste_with_shadow(base: Image.Image, overlay: Image.Image, xy: tuple[int, int], shadow=(0, 0, 0, 190)) -> None:
    x, y = xy
    alpha = overlay.getchannel("A")
    shadow_layer = Image.new("RGBA", overlay.size, shadow)
    shadow_layer.putalpha(alpha.filter(ImageFilter.GaussianBlur(14)))
    base.alpha_composite(shadow_layer, (x + 10, y + 12))
    base.alpha_composite(overlay, xy)


def draw_text_shadow(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font_obj: ImageFont.FreeTypeFont,
    fill: tuple[int, int, int, int],
    anchor: str = "la",
    stroke: int = 0,
) -> None:
    x, y = xy
    draw.text((x + 5, y + 7), text, font=font_obj, fill=(0, 0, 0, 210), anchor=anchor)
    draw.text((x, y), text, font=font_obj, fill=fill, anchor=anchor, stroke_width=stroke, stroke_fill=(2, 10, 20, 210))


def draw_rule(draw: ImageDraw.ImageDraw, x: int, y: int, width: int, color: tuple[int, int, int, int]) -> None:
    draw.rounded_rectangle((x, y, x + width, y + 10), radius=5, fill=color)
    draw.rounded_rectangle((x, y + 18, x + width // 2, y + 22), radius=2, fill=(255, 255, 255, 180))


def render_cover() -> None:
    canvas_size = (2550, 3300)
    base = cover_resize(Image.open(COVER_BG).convert("RGB"), canvas_size).convert("RGBA")
    base.alpha_composite(gradient_overlay(canvas_size, 210, 35, (2, 10, 22)))
    base.alpha_composite(gradient_overlay(canvas_size, 15, 210, (0, 0, 0)))

    draw = ImageDraw.Draw(base)
    title_font = font(SERIF_BOLD, 240)
    title_font_2 = font(SERIF_BOLD, 260)
    subtitle_font = font(SERIF, 74)
    author_font = font(SYMBOL_TEXT, 70)

    logo = mask_as_tint(580, WHITE, 0.78)
    panel = Image.new("RGBA", (logo.width + 90, logo.height + 70), (0, 0, 0, 0))
    pdraw = ImageDraw.Draw(panel)
    pdraw.rounded_rectangle((0, 0, panel.width, panel.height), radius=36, fill=(2, 10, 20, 150), outline=(126, 222, 255, 150), width=3)
    panel.alpha_composite(logo, (45, 35))
    paste_with_shadow(base, panel, (canvas_size[0] - panel.width - 150, 150))

    draw_rule(draw, 180, 310, 580, CYAN)
    draw_text_shadow(draw, (180, 430), "Sail Rust", title_font, WHITE, stroke=2)
    draw_text_shadow(draw, (180, 710), "Book", title_font_2, WHITE, stroke=2)
    draw.text((185, 1008), SUBTITLE, font=subtitle_font, fill=(197, 234, 246, 245))

    footer_h = 330
    footer = Image.new("RGBA", (canvas_size[0], footer_h), (2, 10, 20, 190))
    base.alpha_composite(footer, (0, canvas_size[1] - footer_h))
    draw = ImageDraw.Draw(base)
    draw.text((180, canvas_size[1] - 220), AUTHOR, font=author_font, fill=WHITE)
    draw.text((180, canvas_size[1] - 125), "First Pair Press", font=font(SANS_BOLD, 44), fill=AMBER)
    draw.text((180, canvas_size[1] - 70), "A codebase-first edition with PDF, EPUB, HTML, and Obsidian Vault formats", font=font(SANS, 38), fill=(207, 232, 240, 235))

    base.convert("RGB").save(COVER_OUT, quality=95)


def render_headboard() -> None:
    canvas_size = (2400, 1350)
    base = cover_resize(Image.open(HEADBOARD_BG).convert("RGB"), canvas_size).convert("RGBA")
    base.alpha_composite(side_gradient(canvas_size, 220, 20, (2, 10, 22)))
    base.alpha_composite(gradient_overlay(canvas_size, 70, 25, (0, 0, 0)))

    draw = ImageDraw.Draw(base)
    eyebrow_font = font(SANS_BOLD, 44)
    title_font = font(SERIF_BOLD, 144)
    subtitle_font = font(SERIF, 58)
    author_font = font(SYMBOL_TEXT, 52)

    draw_rule(draw, 120, 130, 460, AMBER)
    draw.text((120, 190), "FIRST PAIR PRESS ANNOUNCES", font=eyebrow_font, fill=AMBER)
    draw_text_shadow(draw, (120, 285), TITLE, title_font, WHITE, stroke=2)
    draw.text((126, 470), SUBTITLE, font=subtitle_font, fill=(204, 235, 246, 245))
    draw.text((126, 560), AUTHOR, font=author_font, fill=(243, 249, 252, 245))

    logo = mask_as_tint(570, WHITE, 0.82)
    panel = Image.new("RGBA", (logo.width + 80, logo.height + 60), (0, 0, 0, 0))
    pdraw = ImageDraw.Draw(panel)
    pdraw.rounded_rectangle((0, 0, panel.width, panel.height), radius=32, fill=(2, 10, 20, 150), outline=(126, 222, 255, 140), width=3)
    panel.alpha_composite(logo, (40, 30))
    paste_with_shadow(
        base,
        panel,
        (canvas_size[0] - panel.width - 110, canvas_size[1] - panel.height - 95),
    )

    base.convert("RGB").save(HEADBOARD_OUT, quality=95)


def main() -> int:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    render_cover()
    render_headboard()
    print(COVER_OUT)
    print(HEADBOARD_OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
