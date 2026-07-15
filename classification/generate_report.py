"""
generate_report.py

Purpose:
    Builds the final PDF report required by Step 4d of your assignment.
    For each repository, it produces:
        a. A histogram of primary ISIC classes (full class names as bin
           labels, count on top of each bar, saved as real vector
           graphics so it can be zoomed in without pixelating)
        b. A rank-ordered table of the top 20 classes with their counts
        c. A short "comments on findings" section, auto-drafted from
           what your classifications actually show (including the known
           methodology limitations you found while building this)

How to use it:
    pip install matplotlib reportlab pypdf
    python generate_report.py

    Produces: classification_report.pdf (in the classification folder)
"""

import sqlite3
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # no GUI needed, just rendering to file
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
)

from pypdf import PdfReader, PdfWriter

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "23158572-sq26-classification.db"
WORK_DIR = Path(__file__).resolve().parent / "_report_temp"
FINAL_OUTPUT = Path(__file__).resolve().parent / "classification_report.pdf"


def simplify_class_label(full_label):
    """
    Classifications are stored like:
      'A01 - Crop and animal production... (Section A: Agriculture...)'

    The assignment (Step 4d) specifically says to use the FULL CLASS NAME
    as the bin/table label - this means the descriptive title text from
    the professor's shared taxonomy spreadsheet, not the short code. So
    we strip both the leading "CODE - " and the trailing
    "(Section ...)" parts, keeping only the title itself, e.g.:
      "Crop and animal production, hunting and related service activities"
    """
    if not full_label:
        return None

    label = full_label
    if " (Section " in label:
        label = label.split(" (Section ")[0]

    # Strip the leading "CODE - " prefix (e.g. "A01 - ")
    if " - " in label:
        prefix, rest = label.split(" - ", 1)
        # Only strip if prefix really looks like a division code
        # (1 letter + 2 digits, e.g. "A01"), so we never accidentally
        # cut a title that happens to contain " - " itself.
        if len(prefix) == 3 and prefix[0].isalpha() and prefix[1:].isdigit():
            label = rest

    return label


def get_repository_identifier(conn, repository_id):
    row = conn.execute(
        "SELECT repository_url FROM projects WHERE repository_id = ? "
        "AND repository_url IS NOT NULL LIMIT 1",
        (repository_id,)
    ).fetchone()
    if row and row[0]:
        url = row[0].lower()
        if "aussda" in url:
            return f"Repository {repository_id} (AUSSDA)"
        if "ukdataservice" in url or "ukds" in url:
            return f"Repository {repository_id} (UKDS)"
    return f"Repository {repository_id}"


def get_repository_data(conn, repository_id):
    """
    Returns:
        project_type_counts: dict e.g. {'QD_PROJECT': 14, 'NOT_A_PROJECT': 4}
        class_counter: Counter of simplified primary_class strings
        low_confidence_count: how many of those were flagged low confidence
        total_classified: how many projects had a class at all
    """
    type_rows = conn.execute("""
        SELECT type, COUNT(*) FROM projects
        WHERE repository_id = ? GROUP BY type
    """, (repository_id,)).fetchall()
    project_type_counts = dict(type_rows)

    class_rows = conn.execute("""
        SELECT c.primary_class, c.evidence
        FROM classifications c
        JOIN projects p ON p.id = c.project_id
        WHERE p.repository_id = ? AND c.target_type = 'PROJECT'
          AND c.primary_class IS NOT NULL
    """, (repository_id,)).fetchall()

    class_counter = Counter()
    low_confidence_count = 0
    for primary_class, evidence in class_rows:
        simplified = simplify_class_label(primary_class)
        class_counter[simplified] += 1
        if evidence and "LOW CONFIDENCE" in evidence:
            low_confidence_count += 1

    return project_type_counts, class_counter, low_confidence_count, len(class_rows)


def make_histogram_pdf(class_counter, repo_label, output_path):
    """
    Horizontal bar chart (easier to read full class names than vertical
    bars), saved as a real vector PDF - stays crisp at any zoom level.
    """
    if not class_counter:
        # Still produce a page saying "no data" so the report structure
        # stays consistent even for edge-case repositories.
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.text(0.5, 0.5, "No PROJECT-level classifications available "
                           "for this repository.",
                ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.savefig(output_path, format="pdf")
        plt.close(fig)
        return

    # Sort ascending so the biggest bar ends up at the top when plotted
    items = sorted(class_counter.items(), key=lambda x: x[1])
    labels = [label for label, _ in items]
    counts = [count for _, count in items]

    fig_height = max(4, 0.4 * len(labels) + 1.5)
    fig, ax = plt.subplots(figsize=(11, fig_height))

    bars = ax.barh(labels, counts, color="#4472C4")
    ax.set_xlabel("Number of projects")
    ax.set_title(f"Primary ISIC Classes - {repo_label}", fontsize=13,
                 fontweight="bold")

    # Count label on top of (at the end of) each bar
    max_count = max(counts) if counts else 1
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + max_count * 0.01,
                bar.get_y() + bar.get_height() / 2,
                str(count), va="center", fontsize=9)

    ax.set_xlim(0, max_count * 1.15)
    fig.tight_layout()
    fig.savefig(output_path, format="pdf")  # vector output
    plt.close(fig)


def build_ranked_table_flowable(class_counter):
    """Top-20 ranked classes as a reportlab Table."""
    ranked = class_counter.most_common(20)

    data = [["Rank", "Primary Class", "Count"]]
    for i, (label, count) in enumerate(ranked, start=1):
        data.append([str(i), label, str(count)])

    table = Table(data, colWidths=[0.6 * inch, 5.2 * inch, 0.8 * inch])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4472C4")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F2F2F2")]),
    ]))
    return table


def draft_comments(repo_label, project_type_counts, class_counter,
                    low_confidence_count, total_classified):
    """
    Auto-drafts an honest, specific comments section based on the real
    data and the known methodology findings from building this
    classifier - not generic filler text.
    """
    total_projects = sum(project_type_counts.values())
    qd_count = project_type_counts.get("QD_PROJECT", 0)
    not_a_project_count = project_type_counts.get("NOT_A_PROJECT", 0)

    lines = []
    lines.append(
        f"{repo_label} contains {total_projects} project(s) total, of "
        f"which {qd_count} were classified as QD_PROJECT and therefore "
        f"eligible for ISIC classification."
    )
    if not_a_project_count:
        lines.append(
            f"{not_a_project_count} project(s) were classified as "
            f"NOT_A_PROJECT (typically due to failed file downloads) and "
            f"were not passed through the ISIC classifier, per the "
            f"assignment specification."
        )

    if class_counter:
        dominant_class, dominant_count = class_counter.most_common(1)[0]
        lines.append(
            f"The dominant primary ISIC class in this repository is "
            f"\"{dominant_class}\", accounting for {dominant_count} of "
            f"{total_classified} classified project(s)."
        )

    if low_confidence_count:
        pct = round(100 * low_confidence_count / max(total_classified, 1))
        lines.append(
            f"{low_confidence_count} of {total_classified} project-level "
            f"classifications ({pct}%) were flagged as low-confidence "
            f"best-guesses (confidence below the noise-floor threshold "
            f"of 0.06), reflecting the inherent difficulty of mapping "
            f"open-ended qualitative social-science research topics onto "
            f"ISIC's industrial/economic activity taxonomy."
        )

    lines.append(
        "Methodological note: classification was performed using a "
        "weighted keyword-matching (TF-IDF cosine similarity) approach "
        "combining project metadata (title, description, keywords) with "
        "extracted text from primary data files. Project-level results "
        "are aggregated via confidence-weighted voting across "
        "individually classified files. Known limitations identified "
        "during development include: (1) divisions with unusually large "
        "reference vocabularies could historically bias matches before "
        "cosine normalization was applied; (2) boilerplate phrasing "
        "repeated across ISIC division descriptions (e.g. demographic "
        "terms like \"children\") could cause false positives before "
        "targeted stopword filtering; (3) administrative/documentation "
        "files (e.g. codebooks, readme files) were excluded from content "
        "classification as they describe the dataset rather than "
        "constituting primary qualitative content; and (4) projects with "
        "very large numbers of short, individually-weak files can still "
        "outvote a single strongly-confident file, as their combined "
        "vote weight accumulates."
    )

    return " ".join(lines)


def build_report():
    WORK_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    repo_ids = [
        row[0] for row in
        conn.execute("SELECT DISTINCT repository_id FROM projects "
                      "ORDER BY repository_id").fetchall()
    ]

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="ReportBody", parent=styles["Normal"], fontName="Helvetica",
        fontSize=10, leading=14, spaceAfter=10
    ))

    text_pdf_paths = []
    chart_pdf_paths = []

    # --- Title page ---
    title_path = WORK_DIR / "00_title.pdf"
    title_doc = SimpleDocTemplate(str(title_path), pagesize=letter)
    title_story = [
        Spacer(1, 2 * inch),
        Paragraph("Seeding QDArchive - Part 2 Classification Report",
                  styles["Title"]),
        Spacer(1, 0.3 * inch),
        Paragraph("ISIC Rev. 5 Classification Results by Repository",
                  styles["Heading2"]),
        Spacer(1, 0.5 * inch),
        Paragraph("Student ID: 23158572", styles["Normal"]),
    ]
    title_doc.build(title_story)
    text_pdf_paths.append(title_path)

    # --- Per-repository sections ---
    for idx, repo_id in enumerate(repo_ids, start=1):
        repo_label = get_repository_identifier(conn, repo_id)
        project_type_counts, class_counter, low_conf_count, total_classified = \
            get_repository_data(conn, repo_id)

        # Chart (vector PDF)
        chart_path = WORK_DIR / f"chart_{repo_id}.pdf"
        make_histogram_pdf(class_counter, repo_label, chart_path)
        chart_pdf_paths.append(chart_path)

        # Text page(s): heading, ranked table, comments
        text_path = WORK_DIR / f"text_{repo_id}.pdf"
        doc = SimpleDocTemplate(str(text_path), pagesize=letter,
                                 topMargin=0.7 * inch, bottomMargin=0.7 * inch)
        story = [
            Paragraph(repo_label, styles["Heading1"]),
            Spacer(1, 0.15 * inch),
            Paragraph("a. Histogram of Primary ISIC Classes", styles["Heading2"]),
            Paragraph("(See following page for the vector chart.)",
                      styles["ReportBody"]),
            Spacer(1, 0.2 * inch),
            Paragraph("b. Top 20 Primary Classes (Ranked)", styles["Heading2"]),
        ]

        if class_counter:
            story.append(build_ranked_table_flowable(class_counter))
        else:
            story.append(Paragraph(
                "No PROJECT-level classifications available for this "
                "repository.", styles["ReportBody"]
            ))

        story.append(Spacer(1, 0.25 * inch))
        story.append(Paragraph("c. Comments on Findings", styles["Heading2"]))
        story.append(Paragraph(
            draft_comments(repo_label, project_type_counts, class_counter,
                            low_conf_count, total_classified),
            styles["ReportBody"]
        ))

        doc.build(story)
        text_pdf_paths.append(text_path)

    conn.close()

    # --- Merge everything in the right order: title, then for each repo
    # its text page(s) followed immediately by its chart page ---
    writer = PdfWriter()

    def append_pdf(path):
        reader = PdfReader(str(path))
        for page in reader.pages:
            writer.add_page(page)

    append_pdf(text_pdf_paths[0])  # title page
    for repo_idx in range(len(repo_ids)):
        append_pdf(text_pdf_paths[repo_idx + 1])  # +1 to skip title
        append_pdf(chart_pdf_paths[repo_idx])

    with open(FINAL_OUTPUT, "wb") as f:
        writer.write(f)

    # Clean up temp files
    for p in text_pdf_paths + chart_pdf_paths:
        p.unlink(missing_ok=True)
    try:
        WORK_DIR.rmdir()
    except OSError:
        pass  # not empty for some reason - harmless, leave it

    print(f"Report saved to: {FINAL_OUTPUT}")
    print(f"Repositories included: {len(repo_ids)}")


if __name__ == "__main__":
    build_report()