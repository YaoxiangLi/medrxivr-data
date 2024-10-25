library(medrxivr)
library(dplyr)

df <- medrxivr::mx_api_content()

df$link <- gsub("https://www.medrxiv.org", "", df$link_page)
df$pdf <- gsub("https://www.medrxiv.org", "", df$link_pdf)

df <- df %>%
  dplyr::select(-c("link_page","link_pdf"))

# Function to split the dataframe into chunks smaller than 100 MB
split_and_save <- function(df, prefix, max_size_mb = 100) {
  # Estimate the size of one row
  row_size <- object.size(df[1, ])
  
  # Calculate the maximum number of rows per chunk to stay under the limit
  max_rows <- floor((max_size_mb * 1024 * 1024) / row_size)
  
  # Split the dataframe and save each part
  num_chunks <- ceiling(nrow(df) / max_rows)
  for (i in seq_len(num_chunks)) {
    start_row <- ((i - 1) * max_rows) + 1
    end_row <- min(i * max_rows, nrow(df))
    chunk <- df[start_row:end_row, ]
    
    # Save each chunk as a separate CSV file
    file_name <- paste0(prefix, "_part", i, ".csv")
    write.csv(chunk, file_name, fileEncoding = "UTF-8", row.names = FALSE)
  }
}

# Save the dataframe in parts
split_and_save(df, "snapshot")

# Save the current timestamp
current_time <- format(Sys.time(), "%Y-%m-%d %H:%M")
writeLines(current_time, "timestamp.txt")
