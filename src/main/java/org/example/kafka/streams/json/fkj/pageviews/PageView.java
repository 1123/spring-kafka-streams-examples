package org.example.kafka.streams.json.fkj.pageviews;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageView {

    private Integer pageId;
    private String time;

}
