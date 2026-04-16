source("10834_sx01_en_v1_0.R")

load("objects_fp-pop_020.RData")

devtools::install_github("koheiw/LSX")

p_load(doParallel, # enable parallelizing
       quanteda, quanteda.textstats, # text mining
       LSX, # latent semantic scaling
       devtools, # install packages from devtools
       tidyverse, # data wrangling and plotting
       stm, # structural topic modeling
       RColorBrewer, # color palettes
       ggsci, # another color palette, see https://cran.r-project.org/web/packages/ggsci/vignettes/ggsci.html
       ggthemes, # ggplot themes
       ggrepel, # avoid overlapping text labels
       ggpmisc, # label plot with the equation of a fitted polynomial or other types of models
       GGally, # adding several functions to reduce the complexity of combining geometric objects with transformed data
       showtext, #  using non-standard fonts in R graphs (extrafonts)
       Cairo, # embed fonts in graphs
       geometry
)

# loading packages for bbplot
# devtools::install_github('bbc/bbplot')
p_load(gapminder, # combines data from multiple sources into coherent time-series 
       ggalt,
       forcats,
       R.utils,
       png,
       grid,
       ggpubr,
       scales,
       bbplot,
       ggiraph,
       gganimate)

# detect number of cores
cores <- detectCores()-2
# set threads to number of cores minus 2
quanteda_options(threads = cores)

# transform dataset to corpus----
corp <- corpus(dat_POP)

# reshape to sentence
toks_sent <- corp %>% 
  corpus_reshape("sentences") %>% 
  tokens(remove_punct = TRUE, remove_symbols = TRUE, remove_url = TRUE)

# display number of documents in corpus
ndoc(toks_sent) # 5137

save(toks_sent, file = "objects_fp-pop_060_toks.RData")


# combine compound words with underscore----
# check for compound words 

head(kwic(toks_sent, pattern = phrase("Vereinte? Nationen")))
head(kwic(toks_sent, pattern = phrase("Rat der EU")))

comp_man_toks <- tokens_replace(toks_sent, phrase(c("Vereinte? Nationen", "Rat der EU", "Vereinigte Staaten")),
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
comp_toks_sent <- tokens_compound(comp_man_toks, pattern = col)

comp_toks_sent <- tokens_tolower(comp_toks_sent) %>%
  tokens_remove(stopwords("de")) %>%
  tokens_remove(stopwords("de")) %>%
  tokens_remove(c("mag", "dr", "dr.in", "mag.a", "univ prof", "mag dr",
                  "utl", "ots"),
                valuetype = "regex")


# remove tokens that have less than 3 characters (e.g., numbers or single characters like "s")
comp_toks_sent <- tokens_select(comp_toks_sent, min_nchar=3L)

# test compound words
head(kwic(comp_toks_sent, pattern = phrase("Kurz")))
head(kwic(comp_toks_sent, pattern = phrase("sicherheit zusammenarbeit*")))
head(kwic(comp_toks_sent, pattern = phrase("IWF")))
head(kwic(comp_toks_sent, pattern = phrase("USA")))
head(kwic(comp_toks_sent, pattern = phrase("IAEA")))
head(kwic(comp_toks_sent, pattern = phrase("kommission*")))
head(kwic(comp_toks_sent, pattern = phrase("pompeo")))

save(comp_toks_sent, toks_sent, dat_POP, file = "objects_fp-pop_060.RData")

# convert to document-feature-matrix
dfmt_sent <- comp_toks_sent %>% 
  dfm(remove_padding = TRUE) %>%
  dfm_select("^\\p{L}+$", valuetype = "regex", min_nchar = 2) %>% 
  dfm_trim(min_termfreq = 2)

fontfamily <- "Ubuntu"
man_col <- pal_lancet("lanonc")(9) # Lancet

# lsx for people----

# load results of slda people
slda_people <- readtext::readtext("./results/results_topic01_people.txt")
slda_people <- unlist(strsplit(slda_people$text, "\\s+"))
slda_people <-  gsub(',', '', slda_people)

# extract sentences for topic people
people <- char_context(toks_sent, slda_people, p = 0.05)

sentiments_people <- dictionary(file = "10834_om05_en_v1_0.yml")
lsx_people <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_people), 
                               terms = slda_people, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_people), 20)
# most negative words
tail(coef(lsx_people), 20)

textplot_terms(lsx_people, 
               highlighted = c("subsidiarität", "zusammenarbeit"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_people, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend
plot_sent_people <- ggplot(dat, aes(x = date, y = fit)) +
                      geom_point(alpha = 0.1, fill = "white") +
                      scale_x_date(date_breaks = "1 year", 
                                   labels = date_format("%Y"),
                                   limits = c(min(dat$date), max(dat$date))) +
                      ylim(-3, 3) +
                      geom_line(data = dat_sm, aes(x = date, y = fit),
                                color = man_col[6], size = 1) +
                      geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
                                alpha = 0.7, linetype = 2) +
                      geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
                                alpha = 0.7, linetype = 2) +
                      
                      # zero line
                      geom_hline(yintercept = 0, alpha = 0.5) +
                      
                      # adding begining of Kern cabinet
                      geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
                      annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
                               hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
                      
                      # adding begining of Kurz I cabinet
                      geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
                      annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
                               hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
                      
                      # adding begining of Kurz II cabinet
                      geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
                      annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
                               hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
                      
                      # adding begining of Schallenberg/Nehammer cabinet
                      annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
                               hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
                      
                      bbc_style() +
          
                      xlab("time") +
                      ylab("people sentiment") +
                      
                      theme(plot.title = element_text(size = 16, family = fontfamily),
                            plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
                            panel.grid.major.x = element_line(linetype = "dotted"),
                            panel.grid.minor.x = element_blank(),
                            panel.grid.minor.y = element_blank(), 
                            plot.background = element_blank(),
                            axis.ticks.y = element_blank(),
                            panel.grid.major.y = element_blank(),
                            #panel.grid.major.y = element_line(linetype = "dotted"),
                            axis.text.x = element_text(size = 12, family = fontfamily),
                            axis.text.y = element_text(size = 12, family = fontfamily),
                            axis.title.x = element_text(size = 12, family = fontfamily),
                            axis.title.y = element_text(size = 12, family = fontfamily),
                            legend.position = "none")

ggsave(filename = "./results/plot_sentiments_people.pdf",
       plot = plot_sent_people, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)


# lsx for diversity----

# load results of slda diversity
slda_diversity <- readtext::readtext("./results/results_topic04_diversity.txt")
slda_diversity <- unlist(strsplit(slda_diversity$text, "\\s+"))
slda_diversity <-  gsub(',', '', slda_diversity)

# extract sentences for topic diversity
diversity <- char_context(toks_sent, slda_diversity, p = 0.05)

sentiments_diversity <- dictionary(file = "10834_om02_en_v1_0.yml")
lsx_diversity <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_diversity), 
                            terms = slda_diversity, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_diversity), 20)
# most negative words
tail(coef(lsx_diversity), 20)

textplot_terms(lsx_diversity, 
               highlighted = c("islamistisch", "werte"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_diversity, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend

plot_sent_diversity <- ggplot(dat, aes(x = date, y = fit)) +
  geom_point(alpha = 0.1, fill = "white") +
  scale_x_date(date_breaks = "1 year", 
               labels = date_format("%Y"),
               limits = c(min(dat$date), max(dat$date))) +
  ylim(-3, 3) +
  geom_line(data = dat_sm, aes(x = date, y = fit),
            color = man_col[2], size = 1) +
  geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  
  # zero line
  geom_hline(yintercept = 0, alpha = 0.5) +
  
  # adding begining of Kern cabinet
  geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
  annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz I cabinet
  geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
  annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz II cabinet
  geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
  annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Schallenberg/Nehammer cabinet
  annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
           hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
  
  bbc_style() +
  
  xlab("time") +
  ylab("diversity sentiment") +
  
  theme(plot.title = element_text(size = 16, family = fontfamily),
        plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
        panel.grid.major.x = element_line(linetype = "dotted"),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(), 
        plot.background = element_blank(),
        axis.ticks.y = element_blank(),
        panel.grid.major.y = element_blank(),
        #panel.grid.major.y = element_line(linetype = "dotted"),
        axis.text.x = element_text(size = 12, family = fontfamily),
        axis.text.y = element_text(size = 12, family = fontfamily),
        axis.title.x = element_text(size = 12, family = fontfamily),
        axis.title.y = element_text(size = 12, family = fontfamily),
        legend.position = "none")

ggsave(filename = "./results/plot_sentiments_diversity.pdf",
       plot = plot_sent_diversity, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)

# lsx for governance----

# load results of slda governance
slda_governance <- readtext::readtext("./results/results_topic03_governance.txt")
slda_governance <- unlist(strsplit(slda_governance$text, "\\s+"))
slda_governance <-  gsub(',', '', slda_governance)

# extract sentences for topic governance
governance <- char_context(toks_sent, slda_governance, p = 0.05)

sentiments_governance <- dictionary(file = "10834_om04_en_v1_0.yml")
lsx_governance <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_governance), 
                               terms = slda_governance, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_governance), 20)
# most negative words
tail(coef(lsx_governance), 20)

textplot_terms(lsx_governance, 
               highlighted = c("frieden", "multilateralismus"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_governance, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend

plot_sent_governance <- ggplot(dat, aes(x = date, y = fit)) +
  geom_point(alpha = 0.1, fill = "white") +
  scale_x_date(date_breaks = "1 year", 
               labels = date_format("%Y"),
               limits = c(min(dat$date), max(dat$date))) +
  ylim(-3, 3) +
  geom_line(data = dat_sm, aes(x = date, y = fit),
            color = man_col[4], size = 1) +
  geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  
  # zero line
  geom_hline(yintercept = 0, alpha = 0.5) +
  
  # adding begining of Kern cabinet
  geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
  annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz I cabinet
  geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
  annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz II cabinet
  geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
  annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Schallenberg/Nehammer cabinet
  annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
           hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
  
  bbc_style() +
  
  xlab("time") +
  ylab("governance sentiment") +
  
  theme(plot.title = element_text(size = 16, family = fontfamily),
        plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
        panel.grid.major.x = element_line(linetype = "dotted"),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(), 
        plot.background = element_blank(),
        axis.ticks.y = element_blank(),
        panel.grid.major.y = element_blank(),
        #panel.grid.major.y = element_line(linetype = "dotted"),
        axis.text.x = element_text(size = 12, family = fontfamily),
        axis.text.y = element_text(size = 12, family = fontfamily),
        axis.title.x = element_text(size = 12, family = fontfamily),
        axis.title.y = element_text(size = 12, family = fontfamily),
        legend.position = "none")

ggsave(filename = "./results/plot_sentiments_governance.pdf",
       plot = plot_sent_governance, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)

# lsx for globalization----

# load results of slda globalization
slda_globalization <- readtext::readtext("./results/results_topic02_globalization.txt")
slda_globalization <- unlist(strsplit(slda_globalization$text, "\\s+"))
slda_globalization <-  gsub(',', '', slda_globalization)

# extract sentences for topic globalization
globalization <- char_context(toks_sent, slda_globalization, p = 0.05)

sentiments_globalization <- dictionary(file = "10834_om03_en_v1_0.yml")
lsx_globalization <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_globalization), 
                               terms = slda_globalization, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_globalization), 20)
# most negative words
tail(coef(lsx_globalization), 20)

textplot_terms(lsx_globalization, 
               highlighted = c("islamistisch", "werte"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_globalization, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend

plot_sent_globalization <- ggplot(dat, aes(x = date, y = fit)) +
  geom_point(alpha = 0.1, fill = "white") +
  scale_x_date(date_breaks = "1 year", 
               labels = date_format("%Y"),
               limits = c(min(dat$date), max(dat$date))) +
  ylim(-3, 3) +
  geom_line(data = dat_sm, aes(x = date, y = fit),
            color = man_col[3], size = 1) +
  geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  
  # zero line
  geom_hline(yintercept = 0, alpha = 0.5) +
  
  # adding begining of Kern cabinet
  geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
  annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz I cabinet
  geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
  annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz II cabinet
  geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
  annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Schallenberg/Nehammer cabinet
  annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
           hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
  
  bbc_style() +
  
  xlab("time") +
  ylab("globalization sentiment") +
  
  theme(plot.title = element_text(size = 16, family = fontfamily),
        plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
        panel.grid.major.x = element_line(linetype = "dotted"),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(), 
        plot.background = element_blank(),
        axis.ticks.y = element_blank(),
        panel.grid.major.y = element_blank(),
        #panel.grid.major.y = element_line(linetype = "dotted"),
        axis.text.x = element_text(size = 12, family = fontfamily),
        axis.text.y = element_text(size = 12, family = fontfamily),
        axis.title.x = element_text(size = 12, family = fontfamily),
        axis.title.y = element_text(size = 12, family = fontfamily),
        legend.position = "none")

ggsave(filename = "./results/plot_sentiments_globalization.pdf",
       plot = plot_sent_globalization, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)

# lsx for usa----

# load results of slda usa
slda_usa <- readtext::readtext("./results/results_topic05_usa.txt")
slda_usa <- unlist(strsplit(slda_usa$text, "\\s+"))
slda_usa <-  gsub(',', '', slda_usa)

# extract sentences for topic usa
usa <- char_context(toks_sent, slda_usa, p = 0.05)

sentiments_usa <- dictionary(file = "10834_om07_en_v1_0.yml")
lsx_usa <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_usa), 
                               terms = slda_usa, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_usa), 20)
# most negative words
tail(coef(lsx_usa), 20)

textplot_terms(lsx_usa, 
               highlighted = c("islamistisch", "werte"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_usa, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend

plot_sent_usa <- ggplot(dat, aes(x = date, y = fit)) +
  geom_point(alpha = 0.1, fill = "white") +
  scale_x_date(date_breaks = "1 year", 
               labels = date_format("%Y"),
               limits = c(min(dat$date), max(dat$date))) +
  ylim(-3, 3) +
  geom_line(data = dat_sm, aes(x = date, y = fit),
            color = man_col[8], size = 1) +
  geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  
  # zero line
  geom_hline(yintercept = 0, alpha = 0.5) +
  
  # adding begining of Kern cabinet
  geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
  annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz I cabinet
  geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
  annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz II cabinet
  geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
  annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Schallenberg/Nehammer cabinet
  annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
           hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
  
  bbc_style() +
  
  xlab("time") +
  ylab("usa sentiment") +
  
  theme(plot.title = element_text(size = 16, family = fontfamily),
        plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
        panel.grid.major.x = element_line(linetype = "dotted"),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(), 
        plot.background = element_blank(),
        axis.ticks.y = element_blank(),
        panel.grid.major.y = element_blank(),
        #panel.grid.major.y = element_line(linetype = "dotted"),
        axis.text.x = element_text(size = 12, family = fontfamily),
        axis.text.y = element_text(size = 12, family = fontfamily),
        axis.title.x = element_text(size = 12, family = fontfamily),
        axis.title.y = element_text(size = 12, family = fontfamily),
        legend.position = "none")

ggsave(filename = "./results/plot_sentiments_usa.pdf",
       plot = plot_sent_usa, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)

# lsx for russia----

# load results of slda russia
slda_russia <- readtext::readtext("./results/results_topic06_russia.txt")
slda_russia <- unlist(strsplit(slda_russia$text, "\\s+"))
slda_russia <-  gsub(',', '', slda_russia)

# extract sentences for topic russia
russia <- char_context(toks_sent, slda_russia, p = 0.05)

sentiments_russia <- dictionary(file = "10834_om06_en_v1_0.yml")
lsx_russia <- textmodel_lss(dfmt_sent, as.seedwords(sentiments_russia), 
                               terms = slda_russia, k = 300, cache = TRUE)

# most positive words
head(coef(lsx_russia), 20)
# most negative words
tail(coef(lsx_russia), 20)

textplot_terms(lsx_russia, 
               highlighted = c("kriegsverbrechen", "dank"))

# time series

dfmt <- dfm_group(dfmt_sent)

dat <- docvars(dfmt)

# predict sentiment scores
dat$fit <- predict(lsx_russia, newdata = dfmt)

# smooth LSS scores
dat_sm <- smooth_lss(dat, span = 0.2, from = min(dat$date), to = max(dat$date))

# plot trend

plot_sent_russia <- ggplot(dat, aes(x = date, y = fit)) +
  geom_point(alpha = 0.1, fill = "white") +
  scale_x_date(date_breaks = "1 year", 
               labels = date_format("%Y"),
               limits = c(min(dat$date), max(dat$date))) +
  ylim(-3, 3) +
  geom_line(data = dat_sm, aes(x = date, y = fit),
            color = man_col[7], size = 1) +
  geom_line(data = dat_sm, aes(x = date, y = fit + se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  geom_line(data = dat_sm, aes(x = date, y = fit - se.fit * 1.96),
            alpha = 0.7, linetype = 2) +
  
  # zero line
  geom_hline(yintercept = 0, alpha = 0.5) +
  
  # adding begining of Kern cabinet
  geom_vline(xintercept = as.Date("2017-12-18"), linetype = "dotted") +
  annotate("text", x = as.Date("2017-01-01"), 3, label = "Kern", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz I cabinet
  geom_vline(xintercept = as.Date("2019-05-28"), linetype = "dotted") +
  annotate("text", x = as.Date("2018-12-01"), 3, label = "Kurz I", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Kurz II cabinet
  geom_vline(xintercept = as.Date("2021-10-11"), linetype = "dotted") +
  annotate("text", x = as.Date("2020-12-01"), 3, label = "Kurz II", 
           hjust = 1, vjust = 1.5, parse = FALSE, family = fontfamily) +
  
  # adding begining of Schallenberg/Nehammer cabinet
  annotate("text", x = as.Date("2022-07-01"), 3, label = "Schallenberg/\nNehammer", 
           hjust = 0.8, vjust = 1.25, parse = FALSE, family = fontfamily) +
  
  bbc_style() +
  
  xlab("time") +
  ylab("russia sentiment") +
  
  theme(plot.title = element_text(size = 16, family = fontfamily),
        plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
        panel.grid.major.x = element_line(linetype = "dotted"),
        panel.grid.minor.x = element_blank(),
        panel.grid.minor.y = element_blank(), 
        plot.background = element_blank(),
        axis.ticks.y = element_blank(),
        panel.grid.major.y = element_blank(),
        #panel.grid.major.y = element_line(linetype = "dotted"),
        axis.text.x = element_text(size = 12, family = fontfamily),
        axis.text.y = element_text(size = 12, family = fontfamily),
        axis.title.x = element_text(size = 12, family = fontfamily),
        axis.title.y = element_text(size = 12, family = fontfamily),
        legend.position = "none")

ggsave(filename = "./results/plot_sentiments_russia.pdf",
       plot = plot_sent_russia, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)
