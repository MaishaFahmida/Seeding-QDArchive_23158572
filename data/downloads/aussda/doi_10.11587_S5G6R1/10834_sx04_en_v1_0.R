source("10834_sx01_en_v1_0.R")

load("objects_fp-pop_010.RData")

p_load(doParallel, # enable parallelizing
       quanteda, quanteda.textstats, # text mining
       topicmodels, # LDA and CDA topic modeling
       devtools, # install packages from devtools
       spacyr, # R wrapper around the Python spaCy package
       tidyverse # data wrangling and plitting
       )

# detect number of cores
cores <- detectCores()-2
# set threads to number of cores minus 2
quanteda_options(threads = cores)


#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 020. Building and manipulating corpus-------------
#-#-#-#-#-#-#-#-#-#-#-#-#-#

# transform dataset to corpus----
corp <- corpus(dat_POP)

# display number of documents in corpus
ndoc(corp) # 297 

# tokenize corpus
toks <- tokens(corp, remove_punct = TRUE, remove_symbols = TRUE, remove_url = TRUE)

toks <- tokens_subset(toks, ntoken(toks) > 0)
# 297 elements

save(toks, file = "objects_fp-pop_020_toks.RData")

# combine compound words with underscore----
# check for compound words 

head(kwic(toks, pattern = phrase("Vereinte? Nationen")))
head(kwic(toks, pattern = phrase("Rat der EU")))

comp_man_toks <- tokens_replace(toks, phrase(c("Vereinte? Nationen", "Rat der EU", "Vereinigte Staaten")),
                                phrase(c("UNO", "Rat_der_EU", "USA")),
                                case_insensitive = TRUE)

head(kwic(comp_man_toks, pattern = phrase("EU")))
tail(kwic(comp_man_toks, pattern = phrase("EU")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("EU", "Europäische Kommission")),
                                phrase(c("Europäische_Union", "EU_Kommission")),
                                case_insensitive = FALSE)

head(kwic(comp_man_toks, pattern = phrase("Rat_der_EU")))
head(kwic(comp_man_toks, pattern = phrase("Karin")))
head(kwic(comp_man_toks, pattern = phrase("Schallenberg")))
head(kwic(comp_man_toks, pattern = phrase("Michael")))

head(kwic(comp_man_toks, pattern = phrase("Organisation für Sicherheit und Zusammenarbeit")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("Organisation für Sicherheit und Zusammenarbeit in Europa OSZE",
                                                        "Organisation für Sicherheit und Zusammenarbeit in Europa",
                                                        "Organisation für Sicherheit und Zusammenarbeit")),
                                phrase(c("OSCE", "OSCE", "OSCE")),
                                case_insensitive = FALSE)

head(kwic(comp_man_toks, pattern = phrase("Währungsfond*")))
head(kwic(comp_man_toks, pattern = phrase("IMF")))
head(kwic(comp_man_toks, pattern = phrase("Internationalen Atomenergieorganisation")))
head(kwic(comp_man_toks, pattern = phrase("Atomenergie*")))
head(kwic(comp_man_toks, pattern = phrase("International* Atomenergiebehörde")))
head(kwic(comp_man_toks, pattern = phrase("nordatlant*")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("Internationale Währungsfonds",
                                                        "Internationalen Atomenergieorganisation IAEO",
                                                        "Internationalen Atomenergieorganisation",
                                                        "International* Atomenergiebehörde")),
                                phrase(c("IWF", "IAEA", "IAEA", "IAEA")),
                                case_insensitive = FALSE)

head(kwic(comp_man_toks, pattern = phrase("international* org*")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("international* orga*")),
                                phrase(c("internationale_organisationen")),
                                case_insensitive = TRUE)

head(kwic(comp_man_toks, pattern = phrase("internationale_org*")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("Karin Kneissl", "Sebastian Kurz",
                                                        "Alexander Schallenberg", "Michael Linhart")),
                                phrase(c("Kneissl", "Kurz", "Schallenberg", "Linhart")),
                                case_insensitive = FALSE)

head(kwic(comp_man_toks, pattern = phrase("Bosnien und Herzegowina")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("Bosnien und Herzegowina")),
                                phrase(c("Bosnien_Herzegowina")),
                                case_insensitive = FALSE)

head(kwic(comp_man_toks, pattern = phrase("brücke*")))
head(kwic(comp_man_toks, pattern = phrase("schlepper*")))
head(kwic(comp_man_toks, pattern = phrase("kulturell bedingt*")))
head(kwic(comp_man_toks, pattern = phrase("entgegen zu*")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("kulturell bedingte", "kulturell bedingter",
                                                        "entgegen zu treten")),
                                phrase(c("kulturell_bedingt", "kulturell_bedingt", "entgegen_zu_treten")),
                                case_insensitive = TRUE)

head(kwic(comp_man_toks, pattern = phrase("van der")))

comp_man_toks <- tokens_replace(comp_man_toks, phrase(c("alexander van der bellen", "van der bellen")),
                                phrase(c("van_der_bellen", "van_der_bellen")),
                                case_insensitive = TRUE)

head(kwic(comp_man_toks, pattern = phrase("dr.in")))
head(kwic(comp_man_toks, pattern = phrase("univ prof")))
head(kwic(comp_man_toks, pattern = phrase("mag dr")))


# discover collocations automatically
col <- comp_man_toks %>% 
        tokens_remove(stopwords("de")) %>% 
        tokens_remove(c("mag", "dr", "dr.in", "mag.a", "univ prof", "mag dr",
                        "utl", "ots"),
                      valuetype = "regex") %>% 
        tokens_select(pattern = "^[A-Z]", valuetype = "regex", 
                      case_insensitive = FALSE, padding = TRUE) %>%
        textstat_collocations(min_count = 10, size = 2, tolower = TRUE)
head(col)

# automatically compound words (2-grams)
comp_toks <- tokens_compound(comp_man_toks, pattern = col)

comp_toks <- tokens_tolower(comp_toks) %>%
        tokens_remove(stopwords("de")) %>%
        tokens_remove(c("mag", "dr", "dr.in", "mag.a", "univ prof", "mag dr",
                "utl", "ots"),
              valuetype = "regex")

# remove tokens that have less than 3 characters (e.g., numbers or single characters like "s")
comp_toks <- tokens_select(comp_toks, min_nchar=3L)

# test compound words
head(kwic(comp_toks, pattern = phrase("Kurz")))
head(kwic(comp_toks, pattern = phrase("sicherheit zusammenarbeit*")))
head(kwic(comp_toks, pattern = phrase("IWF")))
head(kwic(comp_toks, pattern = phrase("USA")))
head(kwic(comp_toks, pattern = phrase("IAEA")))
head(kwic(comp_toks, pattern = phrase("kommission*")))
head(kwic(comp_toks, pattern = phrase("pompeo")))
head(kwic(comp_toks, pattern = phrase("ots")))

save(comp_toks, dat_POP, file = "objects_fp-pop_020.RData")
quit()
