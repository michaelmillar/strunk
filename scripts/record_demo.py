#!/usr/bin/env python3

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import shutil
import subprocess

FRAMES_DIR = Path("demo_frames")
OUTPUT = Path("assets/demo.mp4")
WIDTH = 1280
HEIGHT = 800
FPS = 1

BG = (13, 13, 13)
FG = (220, 220, 220)
DIM = (120, 120, 120)
GREEN = (74, 222, 128)
BLUE = (96, 165, 250)
ORANGE = (251, 146, 60)
YELLOW = (250, 204, 21)
PURPLE = (192, 132, 252)
CYAN = (103, 232, 249)
RED = (248, 113, 113)


def find_mono_font(size):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        "/usr/share/fonts/truetype/ubuntu/UbuntuMono-R.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def find_mono_bold(size):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/UbuntuMono-B.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return find_mono_font(size)


def find_sans_font(size):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()


def find_sans_bold(size):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
    ]
    for p in paths:
        if Path(p).exists():
            return ImageFont.truetype(p, size)
    return find_sans_font(size)


def make_title_card(title, subtitle, duration):
    frames = []
    img = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(img)

    title_font = find_sans_bold(52)
    sub_font = find_sans_font(22)

    tw = draw.textlength(title, font=title_font)
    draw.text(((WIDTH - tw) / 2, HEIGHT / 2 - 50), title, fill=FG, font=title_font)

    sw = draw.textlength(subtitle, font=sub_font)
    draw.text(((WIDTH - sw) / 2, HEIGHT / 2 + 20), subtitle, fill=DIM, font=sub_font)

    for _ in range(duration * FPS):
        frames.append(img.copy())
    return frames


def make_code_frame(lines, annotation, duration):
    frames = []
    img = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(img)

    code_font = find_mono_font(17)
    anno_font = find_sans_font(20)

    y = 40
    for text, colour in lines:
        if text == "":
            y += 12
            continue
        draw.text((50, y), text, fill=colour, font=code_font)
        y += 24

    if annotation:
        img.paste(Image.new("RGB", (WIDTH, 70), (30, 30, 30)), (0, HEIGHT - 70))
        aw = draw.textlength(annotation, font=anno_font)
        draw.text(((WIDTH - aw) / 2, HEIGHT - 48), annotation, fill=DIM, font=anno_font)

    for _ in range(duration * FPS):
        frames.append(img.copy())
    return frames


def make_terminal_frame(lines, annotation, duration):
    return make_code_frame(lines, annotation, duration)


def make_feature_card(heading, features, duration):
    frames = []
    img = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(img)

    head_font = find_sans_bold(36)
    feat_font = find_mono_font(20)
    desc_font = find_sans_font(17)

    hw = draw.textlength(heading, font=head_font)
    draw.text(((WIDTH - hw) / 2, 60), heading, fill=FG, font=head_font)

    y = 140
    for name, desc in features:
        draw.text((120, y), name, fill=GREEN, font=feat_font)
        draw.text((120, y + 30), desc, fill=DIM, font=desc_font)
        y += 80

    for _ in range(duration * FPS):
        frames.append(img.copy())
    return frames


SCENES = [
    {
        "type": "title",
        "title": "strunk",
        "subtitle": "Durable task queues and state events for Rust services on PostgreSQL",
        "duration": 4,
    },
    {
        "type": "code",
        "annotation": "Business data, tasks, and events commit atomically. Or none of them do.",
        "duration": 7,
        "lines": [
            ("let mut tx = strunk.begin().await?;", FG),
            ("", None),
            ("// business logic", DIM),
            ('sqlx::query("UPDATE orders SET status = \'shipped\' WHERE id = $1")', BLUE),
            ("    .bind(order_id)", BLUE),
            ("    .execute(&mut *tx)", BLUE),
            ("    .await?;", BLUE),
            ("", None),
            ("// queue a background task (same transaction)", DIM),
            ('strunk.task(&mut tx, "notifications")', GREEN),
            ('    .typed(&Notification { order_id, kind: "shipped" })', GREEN),
            ("    .submit()", GREEN),
            ("    .await?;", GREEN),
            ("", None),
            ("// publish entity state (same transaction)", DIM),
            ('strunk.event(&mut tx, "order", &order_id.to_string())', ORANGE),
            ('    .typed(&OrderState { id: order_id, status: "shipped" })', ORANGE),
            ("    .publish()", ORANGE),
            ("    .await?;", ORANGE),
            ("", None),
            ("tx.commit().await?;", YELLOW),
        ],
    },
    {
        "type": "code",
        "annotation": "Compile-time payload safety. Deserialisation failures become poison messages.",
        "duration": 6,
        "lines": [
            ("#[derive(Serialize, Deserialize)]", PURPLE),
            ("struct SendEmail {", FG),
            ("    to: String,", FG),
            ("    template: String,", FG),
            ("}", FG),
            ("", None),
            ("// typed submission", DIM),
            ('strunk.task(&mut tx, "emails")', GREEN),
            ("    .typed(&SendEmail {", GREEN),
            ('        to: "user@example.com".into(),', GREEN),
            ('        template: "welcome".into(),', GREEN),
            ("    })", GREEN),
            ("    .submit().await?;", GREEN),
            ("", None),
            ("// typed worker", DIM),
            ('strunk.worker("emails")', BLUE),
            ("    .concurrency(4)", BLUE),
            ("    .spawn_typed(|task: TypedTask<SendEmail>| async move {", BLUE),
            ("        send_email(&task.data.to, &task.data.template).await?;", BLUE),
            ("        Ok(())", BLUE),
            ("    });", BLUE),
        ],
    },
    {
        "type": "terminal",
        "annotation": "Everything is a SQL query. Inspect with the CLI or your own monitoring.",
        "duration": 5,
        "lines": [
            ("$ strunk stats --queue email-queue", DIM),
            ("", None),
            ("+-------------+---------+---------+-----------+------+----------------+", FG),
            ("| queue       | pending | claimed | delivered | dead | oldest_pending |", FG),
            ("+-------------+---------+---------+-----------+------+----------------+", FG),
            ("| email-queue |      12 |       4 |     1,847 |    3 | 2026-03-29 ... |", FG),
            ("+-------------+---------+---------+-----------+------+----------------+", FG),
            ("", None),
            ("$ strunk health", DIM),
            ("", None),
            ("database:    ok", GREEN),
            ("pending:     12", FG),
            ("oldest (s):  4", FG),
            ("status:      healthy", GREEN),
            ("", None),
            ("$ strunk dead-letters email-queue", DIM),
            ("", None),
            ("+-------+-------------+----------+---------------------+-----------+", FG),
            ("| id    | queue       | attempts | created             | payload   |", FG),
            ("+-------+-------------+----------+---------------------+-----------+", FG),
            ('| 12345 | email-queue |        3 | 2026-03-29 14:02:11 | {"to":... |', FG),
            ("+-------+-------------+----------+---------------------+-----------+", FG),
        ],
    },
    {
        "type": "features",
        "heading": "What makes it different",
        "duration": 6,
        "features": [
            ("LISTEN/NOTIFY wakeup", "Near-zero latency. Workers wake instantly when tasks arrive."),
            ("Typed handlers", "Compile-time payload safety via Serialize/Deserialize."),
            ("Consumer inbox", "Prevents duplicate processing after worker crashes."),
            ("Event replay", "Rewind subscribers to reprocess from any point."),
            ("Schema registry", "Versioned contracts with backward compatibility enforcement."),
            ("Transactional outbox", "Tasks and events commit with your business data. No dual-write gap."),
        ],
    },
    {
        "type": "title",
        "title": "strunk",
        "subtitle": "cargo add strunk",
        "duration": 3,
    },
]


def render_scenes():
    FRAMES_DIR.mkdir(exist_ok=True)
    frame_idx = 0

    for scene in SCENES:
        scene_type = scene["type"]

        if scene_type == "title":
            frames = make_title_card(scene["title"], scene["subtitle"], scene["duration"])
        elif scene_type == "code":
            frames = make_code_frame(scene["lines"], scene.get("annotation"), scene["duration"])
        elif scene_type == "terminal":
            frames = make_terminal_frame(scene["lines"], scene.get("annotation"), scene["duration"])
        elif scene_type == "features":
            frames = make_feature_card(scene["heading"], scene["features"], scene["duration"])
        else:
            continue

        for f in frames:
            f.save(FRAMES_DIR / f"frame_{frame_idx:04d}.png")
            frame_idx += 1

    return frame_idx


def encode_video(total_frames):
    OUTPUT.parent.mkdir(exist_ok=True)
    cmd = [
        "ffmpeg", "-y",
        "-framerate", str(FPS),
        "-i", str(FRAMES_DIR / "frame_%04d.png"),
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-r", "30",
        "-preset", "medium",
        "-crf", "23",
        str(OUTPUT),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    print(f"wrote {OUTPUT} ({OUTPUT.stat().st_size // 1024} KB, {total_frames} frames)")


def cleanup():
    if FRAMES_DIR.exists():
        shutil.rmtree(FRAMES_DIR)


def main():
    print("rendering frames...")
    total = render_scenes()
    print(f"encoding {total} frames...")
    encode_video(total)
    cleanup()
    print("done")


if __name__ == "__main__":
    main()
