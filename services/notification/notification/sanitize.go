package notification

import (
	"strings"
	"unicode"
)

// Display-text sanitising for mail (S-10).
//
// Registration is open and the address is not verified before the Welcome mail
// is sent, so anyone can make the platform mail a stranger a greeting that
// carries text the registrant chose. Course titles (an instructor chooses
// them) and publish-failure reasons are the same class. HTML escaping and
// header encoding already stop injection; what is left is content abuse: a
// phishing sentence or a URL delivered from the platform's own sender and
// domain. So every user-controlled value is reduced to a conservative
// allowlist before it reaches a template, and nothing that looks like a URL,
// a domain or an address survives.
//
// The functions are pure and deterministic. They never fail: an input with
// nothing left yields "" and the caller substitutes a neutral fallback.

const (
	maxDisplayNameLength = 60
	maxActivityLength    = 100
)

// DisplayName is the recipient name as it may appear in a mail body or a To
// header: Unicode letters and marks, spaces, apostrophe, hyphen and period,
// everything else stripped, at most 60 characters. It returns "" when nothing
// is left; Render then greets with a neutral "there".
func DisplayName(s string) string {
	return sanitizeText(s, allowName, maxDisplayNameLength, false)
}

// SafeCourseTitle is DisplayName for an instructor-controlled course title:
// digits and common punctuation are allowed too, URLs, addresses and control
// characters are not. At most maxTemplateFieldLength characters.
func SafeCourseTitle(s string) string {
	return sanitizeText(s, allowTitle, maxTemplateFieldLength, true)
}

// safeReason is the same for a publish-failure reason. The service emits a
// fixed vocabulary ("asset_invalid: asset not found"); this is the guard for
// an event from an older or different producer.
func safeReason(s string) string {
	return sanitizeText(s, allowReason, maxPublishReasonLength, false)
}

// safeActivity is the same for the name of a failed workflow step.
func safeActivity(s string) string {
	return sanitizeText(s, allowActivity, maxActivityLength, false)
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsMark(r) }

func allowName(r rune) bool {
	return isWordRune(r) || r == '\'' || r == '’' || r == '-' || r == '.'
}

func allowTitle(r rune) bool {
	return isWordRune(r) || unicode.IsDigit(r) || strings.ContainsRune("'’-.,:;!?()&+#%/", r)
}

func allowReason(r rune) bool {
	return isWordRune(r) || unicode.IsDigit(r) || strings.ContainsRune("_:-,.()'", r)
}

func allowActivity(r rune) bool {
	return isWordRune(r) || unicode.IsDigit(r) || r == '_'
}

// sanitizeText keeps the runes allow accepts, drops every whitespace-separated
// word that looks like a URL or an address, defangs domain-like text, and caps
// the result at max characters.
//
//   - A word containing "://" or "@", or starting with "www.", is dropped whole.
//   - A period survives only at the end of a word after a letter ("Jr.", "St.")
//     or between two digits ("3.12"). Anywhere else it becomes a space, so
//     "evil.example" reads "evil example" and cannot be linked or copied.
//   - A word with no letter or digit left ("---", "...") is dropped; titles
//     (lonePunct) keep a lone "&", "-" or "+" ("Go - Basics & Beyond").
func sanitizeText(s string, allow func(rune) bool, max int, lonePunct bool) string {
	var words []string
	for _, w := range strings.FieldsFunc(s, unicode.IsSpace) {
		if urlLike(w) {
			continue
		}
		kept := make([]rune, 0, len(w))
		for _, r := range w {
			if allow(r) {
				kept = append(kept, r)
			}
		}
		for i, r := range kept {
			if r != '.' {
				continue
			}
			endOfWord := i == len(kept)-1 && i > 0 && unicode.IsLetter(kept[i-1])
			betweenDigits := i > 0 && i < len(kept)-1 && unicode.IsDigit(kept[i-1]) && unicode.IsDigit(kept[i+1])
			if !endOfWord && !betweenDigits {
				kept[i] = ' '
			}
		}
		for _, part := range strings.Fields(string(kept)) {
			if hasLetterOrDigit(part) || (lonePunct && (part == "&" || part == "-" || part == "+")) {
				words = append(words, part)
			}
		}
	}
	out := strings.Join(words, " ")
	if r := []rune(out); len(r) > max {
		out = strings.TrimSpace(string(r[:max]))
	}
	return out
}

func urlLike(w string) bool {
	l := strings.ToLower(w)
	return strings.Contains(l, "://") || strings.Contains(l, "@") || strings.HasPrefix(l, "www.")
}

func hasLetterOrDigit(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}
