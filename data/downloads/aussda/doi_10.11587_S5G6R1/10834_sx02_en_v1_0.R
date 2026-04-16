source("10834_sx01_en_v1_0.R")

# for mac-users
programs <- read.csv2("data/programs/data_programs.csv")

# for windows users
# programs <- read.csv2("10834_da_de_v1_0/data/programs/data_programs.csv")

programs$category <- factor(programs$category, levels = c("people", "globalization", "transnational governance",
                              "cultural diversity", "USA", "Russia"))

                  
p_load(ggplot2,
      tidyverse, # data wrangling and plotting, 
      ggsci, # another color palette, see https://cran.r-project.org/web/packages/ggsci/vignettes/ggsci.html
      showtext, #  using non-standard fonts in R graphs (extrafonts)
      Cairo # embed fonts in graphs
)

fontfamily <- "Ubuntu"
man_col <- pal_lancet("lanonc")(9) # Lancet
       
ggplot(programs, aes(x = category, y = cabinet, fill = difference)) +
  geom_tile(color = "white", lwd = 1.5, linetype = 1) +
  geom_text(aes(label = paste(abs(positive), "-", abs(negative))), color = "black",
            size = 4, family = fontfamily) + # labeling the positive and negative values
  scale_fill_gradient2(low = man_col[7], mid = "white", high = man_col[1]) +
  coord_fixed() +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust=1, family = fontfamily),
        axis.text.y = element_text(family = fontfamily),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        legend.title = element_text(size = 10, family = fontfamily)) +
  
  labs(fill = "sentiment")

ggsave("./results/plot_programs.pdf",
       device = cairo_pdf)
