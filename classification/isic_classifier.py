"""
isic_classifier.py

Purpose:
    Compares a project's text (title + description + keywords + extracted
    file text) against every ISIC Rev. 5 division's reference vocabulary
    (division title + "includes" + "includes also" text from your
    professor's CSV), and picks the best-matching division.

How the matching works (in plain words):
    1. Turn the project's text into a "bag of words" (ignoring tiny common
       words like "the", "and", "of").
    2. Do the same for every ISIC division's reference text.
    3. Score how much each division's words overlap with the project's
       words (weighted so rarer, more specific words count for more -
       this is the classic "TF-IDF" idea, done here without extra
       libraries so nothing new needs to be installed).
    4. The division with the highest score wins -> primary_class.
    5. If a second division scores close behind, it becomes the
       secondary_class.

Output per classification:
    {
        "primary_class": "Q86 - Human health activities",
        "secondary_class": "Q87 - Residential care activities" or None,
        "confidence": 0.42,              # 0-1, higher = more confident
        "evidence": "top matching words: interview, patient, clinic, ...",
        "classifier_version": "keyword-tfidf-v1",
    }

Run this file directly for a quick manual test:
    python isic_classifier.py
"""

import re
import math
from collections import Counter
from pathlib import Path

from isic_taxonomy import load_taxonomy

CSV_PATH = Path(__file__).resolve().parent / "isic_rev5_structure.csv"
CLASSIFIER_VERSION = "keyword-tfidf-v2"

# Below this confidence, the "winning" division isn't meaningfully better
# than the noise floor - it's honest to flag this rather than present a
# shaky guess as if it were a solid answer. This threshold was chosen by
# looking at real runs: genuine strong matches (e.g. Agriculture for a
# farming-titled project) scored 0.15-0.4+, while noise-level mismatches
# scored under 0.06.
LOW_CONFIDENCE_THRESHOLD = 0.06

# Common English words to ignore - they appear everywhere and carry no
# useful signal for telling divisions apart.
STOPWORDS = {
    "the", "and", "of", "to", "in", "a", "is", "for", "on", "with", "as",
    "by", "an", "or", "at", "from", "this", "that", "are", "be", "it",
    "was", "were", "which", "their", "its", "these", "those", "also",
    "not", "but", "if", "than", "then", "such", "into", "other", "may",
    "can", "will", "has", "have", "had", "been", "including", "included",
    "includes", "include", "excludes", "excluded", "class", "division",
    "group", "section", "activities", "activity", "study", "data",
    "research", "project", "file", "document",
    # Generic academic / survey / publication jargon that causes false
    # matches against unrelated ISIC divisions (e.g. "distribution" and
    # "content" matching Publishing; "sample" matching Retail trade).
    "survey", "questionnaire", "wave", "waves", "sample", "sampling",
    "respondent", "respondents", "participant", "participants",
    "interview", "interviews", "interviewee", "interviewees",
    "transcript", "transcripts", "codebook", "dataset", "datasets",
    "variable", "variables", "table", "tables", "figure", "figures",
    "appendix", "report", "reports", "publication", "publications",
    "published", "publish", "distribution", "distributed", "content",
    "contents", "edition", "version", "abstract", "introduction",
    "methodology", "method", "methods", "analysis", "analyses",
    "results", "findings", "discussion", "conclusion", "reference",
    "references", "author", "authors", "university", "institute",
    "national", "international", "page", "pages", "chapter", "chapters",
    "download", "downloaded", "archive", "repository", "license",
    "copyright", "doi", "url", "http", "https", "www",
    # Generic demographic descriptors used as boilerplate across MANY
    # unrelated ISIC divisions ("for men, women and children" appears in
    # apparel, footwear, food, healthcare, etc.) - they don't help tell
    # divisions apart and were causing false matches (e.g. any project
    # mentioning "children" getting pulled toward apparel manufacturing).
    "men", "women", "man", "woman", "children", "child", "adults", "adult",
    "boys", "girls", "boy", "girl", "male", "female", "people", "person",
    "persons", "individual", "individuals",
}

WORD_RE = re.compile(r"[a-zA-Z]{3,}")  # words of 3+ letters only


def tokenize(text: str) -> list:
    """Turns text into a clean list of lowercase words, dropping stopwords."""
    if not text:
        return []
    words = WORD_RE.findall(text.lower())
    return [w for w in words if w not in STOPWORDS]


def _build_division_reference_text():
    """
    Reads the full CSV (title + includes + includes also) per division,
    to give the classifier richer vocabulary than just the short title.
    Returns: {division_code: "combined reference text", ...}
    """
    import csv

    reference = {}
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("ISIC Rev 5 Code (with Section)") or "").strip()
            if not (len(code) == 3 and code[0].isalpha() and code[1:].isdigit()):
                continue  # only divisions

            parts = [
                row.get("ISIC Rev 5 Title") or "",
                row.get("ISIC Rev 5 Introductory Text") or "",
                row.get("ISIC Rev 5 Includes") or "",
                row.get("ISIC Rev 5 Includes Also") or "",
            ]
            reference[code] = " ".join(parts)

    return reference


class IsicClassifier:
    def __init__(self):
        self.taxonomy = load_taxonomy()
        self.division_reference_text = _build_division_reference_text()

        # Pre-tokenize each division's reference text once.
        self.division_tokens = {
            code: Counter(tokenize(text))
            for code, text in self.division_reference_text.items()
        }

        # Compute document frequency (how many divisions mention each word)
        # so common words across ALL divisions count for less (IDF idea).
        df = Counter()
        for tokens in self.division_tokens.values():
            for word in tokens:
                df[word] += 1
        n_divisions = max(len(self.division_tokens), 1)
        self.idf = {
            word: math.log(n_divisions / (1 + freq)) + 1
            for word, freq in df.items()
        }

        # Pre-compute each division's TF-IDF vector and its norm (length).
        #
        # IMPORTANT: for the division side, we use BINARY presence (1 if
        # a word appears anywhere in the division's reference text, not
        # how many times). Why: ISIC's "Includes" text is often boilerplate
        # that repeats the same generic words across many bullet points -
        # e.g. Division 14 (apparel) says "...for men, women or children"
        # in several different bullets about different garment types. If
        # we counted raw occurrences, "children" would get an artificially
        # inflated weight in division 14 just from repetition, causing any
        # unrelated project that merely mentions "children" to falsely
        # match apparel. Binary presence avoids this: a word counts once
        # per division no matter how many times it's repeated there.
        self.division_vectors = {}
        self.division_norms = {}
        self.division_vocab_size = {}
        for code, tokens in self.division_tokens.items():
            vector = {
                word: 1.0 * self.idf.get(word, 1.0)
                for word in tokens  # unique words only, count ignored
            }
            self.division_vectors[code] = vector
            self.division_norms[code] = math.sqrt(
                sum(w * w for w in vector.values())
            ) or 1.0
            self.division_vocab_size[code] = len(tokens)

    # Divisions with a very small reference vocabulary (e.g. a short,
    # narrowly-worded division like "Veterinary activities") can score an
    # artificially high cosine similarity just from matching 1-2 words,
    # since there's so little text to dilute the match. This constant
    # sets the vocabulary size below which a penalty kicks in, scaled
    # linearly - a division with only 5 words gets a much bigger penalty
    # than one with 18.
    MIN_VOCAB_SIZE_FOR_FULL_SCORE = 20

    def _score(self, project_tokens: Counter, division_code: str) -> float:
        """Cosine similarity between the project's text and one division's
        reference text. Both sides are TF-IDF weighted, and the score is
        normalized by the length of both vectors - this is what keeps a
        division with a huge reference vocabulary (e.g. Retail Trade)
        from automatically winning just by having more words available
        to match against."""
        division_vector = self.division_vectors[division_code]
        division_norm = self.division_norms[division_code]

        project_vector = {
            word: count * self.idf.get(word, 1.0)
            for word, count in project_tokens.items()
        }
        project_norm = math.sqrt(
            sum(w * w for w in project_vector.values())
        ) or 1.0

        dot_product = sum(
            project_vector[word] * division_vector.get(word, 0.0)
            for word in project_vector
        )

        cosine_similarity = dot_product / (project_norm * division_norm)

        # Apply the small-vocabulary penalty (fixes divisions like
        # Veterinary activities spiking from just 1-2 lucky word matches)
        vocab_size = self.division_vocab_size.get(division_code, 0)
        penalty = min(1.0, vocab_size / self.MIN_VOCAB_SIZE_FOR_FULL_SCORE)

        return cosine_similarity * penalty

    def classify(self, text: str) -> dict:
        """
        Main entry point. Give it any combined text (title + description +
        keywords + file text), get back a classification dict.
        """
        return self._classify_tokens(Counter(tokenize(text)))

    def classify_weighted(self, title_desc_keywords: str, file_text: str,
                           title_weight: int = 10) -> dict:
        """
        Classifies using a WEIGHTED combination of two text sources:
          - title_desc_keywords: short, highly meaningful text (repeated
            `title_weight` times so it counts much more than file noise)
          - file_text: the full extracted file text (counted once)

        This fixes a common failure mode where long documents full of
        generic academic words (survey, report, sample, etc.) drown out
        the much more specific signal in the project's own title.
        """
        title_tokens = tokenize(title_desc_keywords)
        file_tokens = tokenize(file_text)

        combined = Counter()
        for _ in range(title_weight):
            combined.update(title_tokens)
        combined.update(file_tokens)

        return self._classify_tokens(combined)

    def _classify_tokens(self, project_tokens: Counter) -> dict:

        if not project_tokens:
            return {
                "primary_class": None,
                "secondary_class": None,
                "confidence": 0.0,
                "evidence": "No usable text to classify.",
                "classifier_version": CLASSIFIER_VERSION,
            }

        scores = {
            code: self._score(project_tokens, code)
            for code in self.division_tokens
        }

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_code, top_score = ranked[0]
        second_code, second_score = ranked[1] if len(ranked) > 1 else (None, 0)

        if top_score == 0:
            return {
                "primary_class": None,
                "secondary_class": None,
                "confidence": 0.0,
                "evidence": "No matching vocabulary found in any division.",
                "classifier_version": CLASSIFIER_VERSION,
            }

        # Normalize confidence to a rough 0-1 scale relative to the total
        # score mass across all divisions (just for readability - not a
        # statistically rigorous probability).
        total_score = sum(s for _, s in ranked) or 1
        confidence = round(top_score / total_score, 3)

        # Only report a secondary_class if it's reasonably close to the top
        secondary = None
        if second_code and second_score >= top_score * 0.6:
            secondary = self._format_division(second_code)

        # Evidence: which words actually matched, most important first
        matched_words = [
            w for w in project_tokens
            if w in self.division_tokens[top_code]
        ]
        matched_words_sorted = sorted(
            matched_words, key=lambda w: self.idf.get(w, 0), reverse=True
        )
        evidence = "Top matching words: " + ", ".join(matched_words_sorted[:8])

        primary_class = self._format_division(top_code)

        # Be honest when the match is weak - this isn't a confident
        # answer, it's the least-bad option among many similarly weak
        # candidates. Flagging this openly is more defensible in a report
        # than silently presenting it as a solid classification.
        low_confidence = confidence < LOW_CONFIDENCE_THRESHOLD
        if low_confidence:
            evidence = (
                f"[LOW CONFIDENCE - best guess only] {evidence}"
            )

        return {
            "primary_class": primary_class,
            "secondary_class": secondary,
            "confidence": confidence,
            "low_confidence": low_confidence,
            "evidence": evidence,
            "classifier_version": CLASSIFIER_VERSION,
        }

    def _format_division(self, division_code: str) -> str:
        title = self.taxonomy["divisions"].get(division_code, "Unknown")
        section_code = division_code[0]
        section_title = self.taxonomy["sections"].get(section_code, "Unknown")
        return f"{division_code} - {title} (Section {section_code}: {section_title})"


if __name__ == "__main__":
    classifier = IsicClassifier()

    # Quick manual test with a made-up example - replace with real text
    # from one of your projects to sanity-check the results.
    sample_text = """
    This project contains interview transcripts from patients discussing
    their experiences with hospital treatment and nursing care during
    their recovery from surgery.
    """

    result = classifier.classify(sample_text)
    print("--- Test classification ---")
    for key, value in result.items():
        print(f"  {key}: {value}")