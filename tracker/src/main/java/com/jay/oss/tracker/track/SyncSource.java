package com.jay.oss.tracker.track;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *  同步源信息
 * </p>
 *
 * @author Jay
 * @date 2022/02/23 11:33
 */
@AllArgsConstructor
@Getter
@Setter
@ToString
public class SyncSource {
    /**
     * 同步源url
     */
    private String url;

    /**
     * 同步源hash起始值
     */
    private int hashStart;

    /**
     * 同步源hash结束值
     */
    private int hashEnd;
}
