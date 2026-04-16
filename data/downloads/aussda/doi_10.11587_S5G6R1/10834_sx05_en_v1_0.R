source("10834_sx01_en_v1_0.R")

load("objects_fp-pop_020.RData")

p_load(doParallel, # enable parallelizing
       quanteda, quanteda.textstats, # text mining
       topicmodels, # LDA and CDA topic modeling
       seededlda, # seeded-lda
       devtools, # install packages from devtools
       tidyverse, # data wrangling and plitting
       ldatuning, # metrics to estimate the best fitting number of topics
       tictoc, # record all timings while a complex script is running
       ggwordcloud, # wordcloud plotting
       RColorBrewer, # color palettes
       ggsci, # another color palette, see https://cran.r-project.org/web/packages/ggsci/vignettes/ggsci.html
       ggthemes, # ggplot themes
       showtext, #  using non-standard fonts in R graphs (extrafonts)
       Cairo # embed fonts in graphs
)

# create results folder
if (file.exists("results")){
} else {
  
  # create a new sub directory inside
  # the main path
  dir.create("results")
}

# detect number of cores
cores <- detectCores()-2
# set threads to number of cores minus 2
quanteda_options(threads = cores)



dfmat_POP <- dfm(comp_toks) %>% 
              dfm_trim(max_docfreq = 0.99, docfreq_type = "prop")
        #dfm_trim(min_termfreq = 0.9, termfreq_type = "quantile",
        #                max_docfreq = 0.1, docfreq_type = "prop")
        #dfm_trim(min_docfreq = 0.025,
        #         max_docfreq = 0.975, docfreq_type = "prop") # cuts off 5 % of total tokens

# convert to dtm
dtm_POP <- convert(dfmat_POP, to = "tm")

rowTotals <- apply(dtm_POP , 1, sum) #Find the sum of words in each Document
dtm_POP   <- dtm_POP[rowTotals> 0, ]           #remove all docs without words

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 030. LDA-------------
#-#-#-#-#-#-#-#-#-#-#-#-#-#

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 031. LDA Analysis
#-#-#-#-#-#-#-#-#-#-#-#-#-#


# finding ideal number of topics
tic("topicnum1")
topicnum1 <-  FindTopicsNumber(
  dtm_POP,
  topics = seq(from = 5, to = 100, by = 5), 
  metrics = c("Griffiths2004", "CaoJuan2009", "Arun2010", "Deveaud2014"),
  method = "Gibbs",
  control = list(seed = 14102014),
  mc.cores = cores,
  verbose = TRUE)
toc()
system("say finished!")

pdf("./results/plot_TopicsNumber_5-100.pdf")
FindTopicsNumber_plot(topicnum1)
dev.off()

# ideal point between 10 and 30 topics

# finding ideal number of topics
tic("topicnum2")
topicnum2 <-  FindTopicsNumber(
  dtm_POP,
  topics = seq(from = 10, to = 30, by = 1), 
  metrics = c("Griffiths2004", "CaoJuan2009", "Arun2010", "Deveaud2014"),
  method = "Gibbs",
  control = list(seed = 14102014),
  mc.cores = cores,
  verbose = TRUE)
toc()
system("say finished!")

pdf("./results/plot_TopicsNumber_10-30.pdf")
FindTopicsNumber_plot(topicnum2)
dev.off()

# LDA with 18 topics
tmod_lda <- textmodel_lda(dfmat_POP, k = 18)

# top 10 terms in topics
terms(tmod_lda, 10)


save(dfmat_POP, dtm_POP, tmod_lda, file = "objects_fp-pop_030.RData")
# load("objects_FL_030.RData")

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 032. LDA Plotting Results----
#-#-#-#-#-#-#-#-#-#-#-#-#-#

terms_df <- as.data.frame(terms(tmod_lda, 20))

# choose font
font_add_google("Roboto", "roboto")
showtext_auto()

ggsave("./results/plot_lda-topics_1-4.pdf",
        terms_df  %>% 
                rownames_to_column(("id")) %>% 
                mutate(id = as.numeric(id)) %>% 
                rename(topic01 = topic1, topic02 = topic2, topic03 = topic3,
                       topic04 = topic4) %>% 
                pivot_longer(-id, names_to = "topic", values_to = "term") %>% 
                filter(topic %in% c("topic01", "topic02", "topic03", "topic04")) %>% 
                ggplot(aes(label = term, size = rev(id), alpha = rev(id), family = "roboto")) +
                geom_text_wordcloud(seed = 080622, family = "roboto") +
                scale_size(range = c(2,5)) + 
                facet_wrap(~topic, scales = "free") +
                scale_alpha_continuous(range = c(0.2, 1)) +
                theme_minimal() +
                theme(strip.background = element_rect(fill = "black"),
                      strip.text.x = element_text(colour = "white")),
        device = cairo_pdf, 
        width = 210, 
        height = 297, 
        units = "mm"
)

ggsave("./results/plot_lda-topics_5-8.pdf",
       terms_df  %>% 
               rownames_to_column(("id")) %>% 
               mutate(id = as.numeric(id)) %>% 
               rename(topic05 = topic5, topic06 = topic6,
                      topic07 = topic7, topic08 = topic8) %>% 
               pivot_longer(-id, names_to = "topic", values_to = "term") %>% 
               filter(topic %in% c("topic05", "topic06", "topic07", "topic08")) %>% 
               ggplot(aes(label = term, size = rev(id), alpha = rev(id), family = "roboto")) +
               geom_text_wordcloud(seed = 080622, family = "roboto") +
               scale_size(range = c(2,5)) + 
               facet_wrap(~topic, scales = "free") +
               scale_alpha_continuous(range = c(0.4, 1)) +
               theme_minimal() +
               theme(strip.background = element_rect(fill = "black"),
                     strip.text.x = element_text(colour = "white")),
       device = cairo_pdf, 
       width = 210, 
       height = 297, 
       units = "mm"
)

ggsave("./results/plot_lda-topics_9-12.pdf",
       terms_df  %>% 
               rownames_to_column(("id")) %>% 
               mutate(id = as.numeric(id)) %>% 
               rename(topic09 = topic9) %>% 
               pivot_longer(-id, names_to = "topic", values_to = "term") %>% 
               filter(topic %in% c("topic09", "topic10", "topic11",
                                   "topic12")) %>% 
               ggplot(aes(label = term, size = rev(id), alpha = rev(id), family = "roboto")) +
               geom_text_wordcloud(seed = 080622, family = "roboto") +
               scale_size(range = c(2,5)) + 
               facet_wrap(~topic, scales = "free") +
               scale_alpha_continuous(range = c(0.4, 1)) +
               theme_minimal() +
               theme(strip.background = element_rect(fill = "black"),
                     strip.text.x = element_text(colour = "white")),
       device = cairo_pdf, 
       width = 210, 
       height = 297, 
       units = "mm"
)

ggsave("./results/plot_lda-topics_13-16.pdf",
       terms_df  %>% 
         rownames_to_column(("id")) %>% 
         mutate(id = as.numeric(id)) %>% 
         rename(topic09 = topic9) %>% 
         pivot_longer(-id, names_to = "topic", values_to = "term") %>% 
         filter(topic %in% c("topic13", "topic14", "topic15", "topic16")) %>% 
         ggplot(aes(label = term, size = rev(id), alpha = rev(id), family = "roboto")) +
         geom_text_wordcloud(seed = 080622, family = "roboto") +
         scale_size(range = c(2,5)) + 
         facet_wrap(~topic, scales = "free") +
         scale_alpha_continuous(range = c(0.4, 1)) +
         theme_minimal() +
         theme(strip.background = element_rect(fill = "black"),
               strip.text.x = element_text(colour = "white")),
       device = cairo_pdf, 
       width = 210, 
       height = 297, 
       units = "mm"
)

ggsave("./results/plot_lda-topics_17-18.pdf",
       terms_df  %>% 
         rownames_to_column(("id")) %>% 
         mutate(id = as.numeric(id)) %>% 
         rename(topic09 = topic9) %>% 
         pivot_longer(-id, names_to = "topic", values_to = "term") %>% 
         filter(topic %in% c("topic17", "topic18")) %>% 
         ggplot(aes(label = term, size = rev(id), alpha = rev(id), family = "roboto")) +
         geom_text_wordcloud(seed = 080622, family = "roboto") +
         scale_size(range = c(2,5)) + 
         facet_wrap(~topic, scales = "free") +
         scale_alpha_continuous(range = c(0.4, 1)) +
         theme_minimal() +
         theme(strip.background = element_rect(fill = "black"),
               strip.text.x = element_text(colour = "white")),
       device = cairo_pdf, 
       width = 210, 
       height = 297, 
       units = "mm"
)

quit()
