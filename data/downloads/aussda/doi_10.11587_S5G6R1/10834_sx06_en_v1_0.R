source("10834_sx01_en_v1_0.R")

load("objects_fp-pop_020.RData")
load("objects_fp-pop_030.RData")

p_load(doParallel, # enable parallelizing
       quanteda, quanteda.textstats, # text mining
       topicmodels, # LDA and CDA topic modeling
       seededlda, # seeded-lda
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
       geometry,
       Rtsne,
       rsvd,
       xtable # for exporting table
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

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 050. sLDA-------------
#-#-#-#-#-#-#-#-#-#-#-#-#-#

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 051. sLDA Analysis
#-#-#-#-#-#-#-#-#-#-#-#-#-#

# looking for top features
topfeat <- topfeatures(dfmat_POP, 300)
write(names(topfeat), "./results/topfeatures.txt")

# load and apply dictionary to dfm
dict <- dictionary(file = "10834_om01_en_v1_0.yml")
dfmat_key <- dfm_lookup(dfmat_POP, dict)


## Run textmodel_seededlda ----

# apply slda
sink(file = "./results/output_fp-pop_050.txt", append = TRUE, split = TRUE)
Sys.time()
set.seed(10041983)
slda <- textmodel_seededlda(dfmat_POP, dict, residual = TRUE) 
Sys.time()
system("say finished!")
terms(slda, 200)
table(topics(slda))
write.csv(table(topics(slda)), "./results/slda-table.csv")


topics_df <- as.data.frame(terms(slda, 300))
write.csv(topics_df, file = "./results/results_topic0s.txt")
top01 <- topics_df$people[1:100]
write(as.character(top01), "./results/results_topic01_people.txt", ncolumns = length(top01), sep = ", ")
top02 <- topics_df$globalization[1:100]
write(as.character(top02), "./results/results_topic02_globalization.txt", ncolumns = length(top02), sep = ", ")
top03 <- topics_df$governance[1:100]
write(as.character(top03), "./results/results_topic03_governance.txt", ncolumns = length(top03), sep = ", ")
top04 <- topics_df$diversity[1:100]
write(as.character(top04), "./results/results_topic04_diversity.txt", ncolumns = length(top04), sep = ", ")
top05 <- topics_df$usa[1:100]
write(as.character(top05), "./results/results_topic05_usa.txt", ncolumns = length(top05), sep = ", ")
top06 <- topics_df$russia[1:100]
write(as.character(top06), "./results/results_topic06_russia.txt", ncolumns = length(top06), sep = ", ")
top07 <- topics_df$disarmament[1:100]
write(as.character(top07), "./results/results_topic07_disarmament.txt", ncolumns = length(top07), sep = ", ")
top08 <- topics_df$humaid[1:100]
write(as.character(top08), "./results/results_topic08_humaid.txt", ncolumns = length(top08), sep = ", ")
top09 <- topics_df$embassy[1:100]
write(as.character(top09), "./results/results_topic09_embassy.txt", ncolumns = length(top09), sep = ", ")
top10 <- topics_df$other[1:100]
write(as.character(top10), "./results/results_topic10_other.txt", ncolumns = length(top10), sep = ", ")

save(slda, dfmat_POP, file = "objects_fp-pop_050.RData")

#-#-#-#-#-#-#-#-#-#-#-#-#-#
# 052. Plotting sLDA Results
#-#-#-#-#-#-#-#-#-#-#-#-#-#

# load("objects_fp-pop_050.RData")

# distribution of topics in overall documents----

df <- as.data.frame(table(topics(slda)))
df <- df %>% 
        rename("topic" = Var1, "frequency" = Freq)
        

fontfamily <- "Ubuntu"

plot_distribution_topics <- ggplot(df, aes(reorder(topic, frequency), frequency, fill = topic)) + 
                                geom_bar(stat = "identity") +
                                scale_fill_jco() +
                                bbc_style() +
                                theme(plot.title = element_text(size = 16, family = fontfamily),
                                      plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
                                      panel.grid.major.x = element_line(linetype = "dotted"),
                                      panel.grid.minor.x = element_blank(),
                                      panel.grid.minor.y = element_blank(), 
                                      plot.background = element_blank(),
                                      axis.ticks.y = element_blank(),
                                      panel.grid.major.y = element_blank(),
                                      #panel.grid.major.y = element_line(linetype = "dotted"),
                                      axis.text = element_text(size = 12, family = fontfamily),
                                      legend.position="none") + coord_flip()

ggsave(filename = "./results/plot_distribution-topics.pdf",
       plot = plot_distribution_topics, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)


# create dataset for plotting timeseries----

# extract theta values----
# read theta results from slda
theta <- as.data.frame(slda$theta)
# add document column
theta$document <- slda$data@docvars$docname_

# extract meta data
meta <- slda$data@docvars %>% 
        rename(document = docname_)

# create theta data table
theta <- full_join(meta, theta)%>%
        select(-docid_, - segid_)

# aggregate theta per Qyear 
theta <- theta %>% group_by(cabinet, Qyear, ministry) %>% dplyr::summarise(theta_people = mean(people, na.rm=TRUE),
                                                                   theta_globalization = mean(globalization, na.rm = TRUE),
                                                                   theta_governance = mean(governance, na.rm=TRUE),
                                                                   theta_diversity = mean(diversity, na.rm = TRUE),
                                                                   theta_usa = mean(usa, na.rm = TRUE),
                                                                   theta_russia = mean(russia, na.rm = TRUE),
                                                                   theta_disarmament = mean(disarmament, na.rm = TRUE),
                                                                   theta_humaid = mean(humaid, na.rm = TRUE),
                                                                   theta_embassy = mean(embassy, na.rm = TRUE),
                                                                   theta_other = mean(other, na.rm = TRUE)
                                                                   ) %>%
        group_by() %>% arrange(cabinet, Qyear, ministry)

# extract topics per document----
topics <- as.data.frame(topics(slda))
topics$document <- slda$data@docvars$docname_
topics <- rename(topics, topic = `topics(slda)`)
topics <- topics %>% 
        mutate('people' = ifelse(topic == "people", 1, 0)) %>% 
        mutate('globalization' = ifelse(topic == "globalization", 1, 0)) %>% 
        mutate('governance' = ifelse(topic == "governance", 1, 0)) %>% 
        mutate('diversity' = ifelse(topic == "diversity", 1, 0)) %>% 
        mutate('usa' = ifelse(topic == "usa", 1, 0)) %>% 
        mutate('russia' = ifelse(topic == "russia", 1, 0)) %>% 
        mutate('disarmament' = ifelse(topic == "disarmament", 1, 0)) %>% 
        mutate('humaid' = ifelse(topic == "humaid", 1, 0)) %>% 
        mutate('embassy' = ifelse(topic == "embassy", 1, 0)) %>% 
        mutate('other' = ifelse(topic == "other", 1, 0)) %>% 
        select(-topic)

# create topics data table
topics <- full_join(meta, topics)%>%
        select(-docid_, - segid_)

# aggregate topics per Qyear 
freq <- topics %>% group_by(cabinet, Qyear, ministry) %>% dplyr::summarise(freq_people = sum(people, na.rm=TRUE),
                                                                   freq_globalization = sum(globalization, na.rm = TRUE),
                                                                   freq_governance = sum(governance, na.rm=TRUE),
                                                                   freq_diversity = sum(diversity, na.rm = TRUE),
                                                                   freq_usa = sum(usa, na.rm = TRUE),
                                                                   freq_russia = sum(russia, na.rm = TRUE),
                                                                   freq_disarmament = sum(disarmament, na.rm = TRUE),
                                                                   freq_humaid = sum(humaid, na.rm = TRUE),
                                                                   freq_embassy = sum(embassy, na.rm = TRUE),
                                                                   freq_other = sum(other, na.rm = TRUE)) %>% 
        group_by() %>% arrange(cabinet, Qyear, ministry)

# calculate proportion of topics per Qyear 
prop <- freq %>% dplyr::summarise(prop_people = freq_people/rowSums(freq[,4:13]),
                                  prop_globalization = freq_globalization/rowSums(freq[,4:13]),
                                  prop_governance = freq_governance/rowSums(freq[,4:13]),
                                  prop_diversity = freq_diversity/rowSums(freq[,4:13]),
                                  prop_usa = freq_usa/rowSums(freq[,4:13]),
                                  prop_russia = freq_russia/rowSums(freq[,4:13]),
                                  prop_disarmament = freq_disarmament/rowSums(freq[,4:13]),
                                  prop_humaid = freq_humaid/rowSums(freq[,4:13]),
                                  prop_embassy = freq_embassy/rowSums(freq[,4:13]),
                                  prop_other = freq_other/rowSums(freq[,4:13]),
                                  cabinet = cabinet,
                                  Qyear = Qyear,
                                  ministry = ministry) %>% 
        group_by() %>% arrange(cabinet, Qyear, ministry)

# combine freq, prop and dt dataset

dt <- full_join(freq, prop)

dt <- full_join(dt, theta)

dt_long <- dt %>% gather(c(4:33), key = "variable", value = "value")

dt_wide <- dt_long %>% select(Qyear, cabinet, ministry, variable, value) %>% 
        unite(ministry, variable, col = "variable") %>% 
        spread(variable, value)


# Exploratory Data Analysis-------------

# Frequency plot----

counts <- "freq"
fontfamily <- "Ubuntu"

df <- dt_long %>%
  filter(str_detect(variable, counts))
  #filter(str_detect(variable, c("security", "environ", "electio", "family")))

gg <- ggplot(df, aes(x = Qyear,
                 y = value, #rank so all lines have the same height
                 color = variable,
                 data_id = variable,
                 group = variable)) +
  #interactive elements of layers (points, lines and text at final)
  geom_line_interactive(size = 1.2,
                        alpha = 0.4) +
  geom_point_interactive(
    aes(tooltip = variable), #specifies tooltip for ggiraph
    fill = "white",
    size = 2.5,
    stroke = 1.5,
    shape = 21) +
  geom_text_interactive(data = df %>% filter(Qyear == max(Qyear)),
                        aes(x = Qyear, y = value,
                            color = variable, 
                            label = variable),
                        check_overlap = T)

gg <- gg +
  labs(x = NULL, y = NULL) +
  scale_x_discrete(labels = c(min(df$Qyear), rep("", 52), max(df$Qyear)),
                     position = "top") +
  scale_color_jco() +
  theme_minimal() +
  theme(
    panel.grid = element_blank(),
    plot.background = element_rect(fill = "gray95", color = NA),
    panel.background = element_rect(fill = "gray95", color = NA),
    axis.text.y = element_blank(),
    axis.text.x = element_text(color = "gray50", size = 10, 
                               face = "bold"),
    #legend.position = "none"
  )

gg <- girafe(ggobj = gg, 
       width_svg = 8, height_svg = 6, #sizes the output plot
       options = list(
         opts_tooltip(
           opacity = 0.8, #opacity of the background box 
           css = "background-color:#4c6061; color:white; padding:10px; border-radius:5px;"),
         opts_hover_inv(css = "stroke-width: 1;opacity:0.6;"),
         opts_hover(css = "stroke-width: 4; opacity: 1;")
       )
)

htmltools::save_html(gg, "./results/plot_freq-variables.html")

# extract colors for plot labels
# man_col <- pal_jco("default")(7) # JCO
 man_col <- pal_lancet("lanonc")(9) # Lancet

df_plot <- dt_long %>%
  filter(str_detect(variable, counts)) %>% 
  filter(!variable %in%  c("freq_other", "freq_embassy"))
 
plot_freq_time_series <- df_plot %>%
                ggplot(aes(x = Qyear, y = value, group = variable, colour = variable)) +
                #scale_color_jco() +
                scale_color_lancet() +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_people"))), 
                          aes(x = Qyear, y = value), size = 1) +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_globalization"))), 
                          aes(x = Qyear, y = value), size = 1) +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_governance"))), 
                    aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_diversity"))), 
                          aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_usa"))), 
                          aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_russia"))), 
                          aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_disarmament"))), 
                          aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                geom_line(data = filter(df_plot, variable == as.character(paste0(counts, "_humaid"))), 
                          aes(x = Qyear, y = value), size = 1, alpha = 0.8)  +
                
                geom_point(shape = 21, fill = "white", size = 2, stroke = 2) +
  
                geom_label(aes(x = "2016 Q4", y = unlist(subset(df_plot, Qyear == "2016 Q4" & variable == as.character(paste0(counts, "_disarmament")), value)) + 0.5,
                               label = "disarmament"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[1], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
                
                geom_label(aes(x = "2016 Q2", y = unlist(subset(df_plot, Qyear == "2016 Q2" & variable == as.character(paste0(counts, "_diversity")), value)) + 0.5,
                               label = "Diversity"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[2], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
             
                geom_label(aes(x = "2018 Q2", y = unlist(subset(df_plot, Qyear == "2018 Q2" & variable == as.character(paste0(counts, "_globalization")), value)) + 0.5,
                               label = "Globalization"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[3], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                geom_label(aes(x = "2018 Q2", y = unlist(subset(df_plot, Qyear == "2018 Q2" & variable == as.character(paste0(counts, "_governance")), value)) + 0.5,
                               label = "Governance"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[4], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                geom_label(aes(x = "2017 Q2", y = unlist(subset(df_plot, Qyear == "2017 Q2" & variable == as.character(paste0(counts, "_humaid")), value)) + 0.5,
                               label = "humanitarian aid"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[5], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                geom_label(aes(x = "2017 Q2", y = unlist(subset(df_plot, Qyear == "2017 Q2" & variable == as.character(paste0(counts, "_people")), value)) + 0.5,
                               label = "People"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[6], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                geom_label(aes(x = "2022 Q2", y = unlist(subset(df_plot, Qyear == "2022 Q2" & variable == as.character(paste0(counts, "_russia")), value)) + 0.5,
                               label = "Russia"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[7], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                geom_label(aes(x = "2018 Q4", y = unlist(subset(df_plot, Qyear == "2018 Q4" & variable == as.character(paste0(counts, "_usa")), value)) + 0.5,
                               label = "USA"), 
                           #hjust = 0, 
                           #vjust = 1, 
                           colour = "white", 
                           #alpha = 0.5,
                           fill = man_col[8], 
                           label.size = NA, 
                           family = fontfamily, 
                           size = 3) +
  
                # adding begining of Kern I cabinet
                geom_vline(xintercept = "2017 Q4", linetype = "dotted") +
                annotate("text", "2017 Q1", 11.5, label = "Kern", 
                         hjust = 1, vjust = 1.5, parse = FALSE) +
  
                # adding begining of Kurz I cabinet
                geom_vline(xintercept = "2019 Q2", linetype = "dotted") +
                annotate("text", "2018 Q3", 11.5, label = "Kurz I", 
                         hjust = 1, vjust = 1.5, parse = FALSE) +
  
                # adding begining of Kurz II cabinet
                geom_vline(xintercept = "2021 Q3", linetype = "dotted") +
                annotate("text", "2020 Q4", 11.5, label = "Kurz II", 
                         hjust = 1, vjust = 1.5, parse = FALSE) +
  
                # adding begining of Schallenberg/Nehammer cabinet
                annotate("text", "2022 Q3", 11.5, label = "Schallenberg/Nehammer", 
                         hjust = 1, vjust = 1.5, parse = FALSE) +
  
                bbc_style() +
                
                theme(plot.title = element_text(size = 16, family = fontfamily),
                      plot.subtitle = element_text(size = 14, family = fontfamily, margin = ggplot2::margin(1, 0, 1, 0)),
                      panel.grid.major.x = element_line(linetype = "dotted"),
                      panel.grid.minor.x = element_blank(),
                      panel.grid.minor.y = element_blank(), 
                      plot.background = element_blank(),
                      axis.ticks.y = element_blank(),
                      panel.grid.major.y = element_blank(),
                      #panel.grid.major.y = element_line(linetype = "dotted"),
                      axis.text.x = element_text(size = 12, family = fontfamily, angle = 45, vjust = 1, hjust=1),
                      axis.text.y = element_text(size = 12, family = fontfamily),
                      legend.position = "none")

ggsave(filename = "./results/plot_freq_time-series.pdf",
       plot = plot_freq_time_series, 
       width = 297, 
       height = 150,
       units = "mm",
       device = cairo_pdf)

quit()




