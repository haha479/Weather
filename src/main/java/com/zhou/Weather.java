package com.zhou;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 天气信息实体类（自定义比较器）
 */
@NoArgsConstructor
@Data
public class Weather implements WritableComparable<Weather> {

    private Integer id;
    private String province;
    private String city;
    private Integer areaCode;
    private String wea;
    private Integer temperature;
    private String windDirection;
   // private String windPower;
    private Integer humidity;
    private LocalDateTime reportTime;
    private String ymd;
    private LocalDateTime createTime;

    /**
     * 按省、区、日期（年月）、温度进行比较
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(Weather o) {
        if (o == null) {
            return 1;
        }
        // 先比较省
        int result = this.getProvince().compareTo(o.getProvince());
        if (result != 0) {
            return result;
        }
        // 再比较区
        result = this.getCity().compareTo(o.getCity());
        if (result != 0) {
            return result;
        }
        // 再比较年月
        result = DateTimeFormatter.ofPattern("yyyyMM").format(this.getReportTime())
                .compareTo(DateTimeFormatter.ofPattern("yyyyMM").format(o.getReportTime()));
        if (result != 0) {
            return result;
        }
        // 再比较温度，-1 倒序
        return this.getTemperature().compareTo(o.getTemperature()) * -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.getId());
        out.writeUTF(this.getProvince());
        out.writeUTF(this.getCity());
        out.writeInt(this.getAreaCode());
        out.writeUTF(this.getWea());
        out.writeInt(this.getTemperature());
        out.writeUTF(this.getWindDirection());
       // out.writeUTF(this.getWindPower());
        out.writeUTF(this.getYmd());
        out.writeInt(this.getHumidity());
        out.writeLong(this.getReportTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli());
        out.writeLong(this.getCreateTime().toInstant(ZoneOffset.ofHours(8)).toEpochMilli());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setId(in.readInt());
        this.setProvince(in.readUTF());
        this.setCity(in.readUTF());
        this.setAreaCode(in.readInt());
        this.setWea(in.readUTF());
        this.setTemperature(in.readInt());
        this.setWindDirection(in.readUTF());
       // this.setWindPower(in.readUTF());
        this.setYmd(in.readUTF());
        this.setHumidity(in.readInt());
        this.setReportTime(LocalDateTime.ofEpochSecond(in.readLong() / 1000, 0, ZoneOffset.ofHours(8)));
        this.setCreateTime(LocalDateTime.ofEpochSecond(in.readLong() / 1000, 0, ZoneOffset.ofHours(8)));
    }

    @Override
    public String toString() {
        return  id +
                "," + province  +
                "," + city +
                "," + areaCode +
                "," + wea  +
                "," + temperature +
                "," + windDirection  +
                "," + humidity +
                "," + reportTime +
                "," + ymd +
                "," + createTime ;
    }
}
