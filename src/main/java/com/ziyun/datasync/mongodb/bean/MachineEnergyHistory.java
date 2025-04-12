package com.ziyun.datasync.mongodb.bean;

import lombok.Data;

/**
 * MachineEnergyHistory数据实体类
 */
@Data
public class MachineEnergyHistory {
    private String _id;
    private Long machine_id;
    private Double energy;
    private Long timestamp;
    private String time;
    private String only;
    private Byte is_bigdata_calculate;
    private String create_time;
    private Integer isDeleted = 0; // 0代表未删除，1代表已删除
}
