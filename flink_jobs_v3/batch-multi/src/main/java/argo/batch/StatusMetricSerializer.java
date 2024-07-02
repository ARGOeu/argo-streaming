package argo.batch;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class StatusMetricSerializer extends TypeSerializerSingleton<StatusMetric> {

    public static final StatusMetricSerializer INSTANCE = new StatusMetricSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StatusMetric createInstance() {
        return new StatusMetric();
    }

    @Override
    public StatusMetric copy(StatusMetric from) {


        return new StatusMetric(
                from.getGroup(),
                from.getFunction(),
                from.getService(),
                from.getHostname(),
                from.getMetric(),
                from.getStatus(),
                from.getTimestamp(),
                from.getDateInt(),
                from.getTimeInt(),
                from.getSummary(),
                from.getMessage(),
                from.getPrevState(),
                from.getPrevTs(),
                from.getActualData(),
                from.getOgStatus(),
                from.getRuleApplied(),
                from.getInfo(),
                from.getTags()
        );
    }

    @Override
    public StatusMetric copy(StatusMetric from, StatusMetric reuse) {
        reuse.setGroup(from.getGroup());
        reuse.setFunction(from.getFunction());
        reuse.setService(from.getService());
        reuse.setHostname(from.getHostname());
        reuse.setMetric(from.getMetric());
        reuse.setStatus(from.getStatus());
        reuse.setTimestamp(from.getTimestamp());
        reuse.setDateInt(from.getDateInt());
        reuse.setTimeInt(from.getTimeInt());
        reuse.setSummary(from.getSummary());
        reuse.setMessage(from.getMessage());
        reuse.setPrevState(from.getPrevState());
        reuse.setPrevTs(from.getPrevTs());
        reuse.setActualData(from.getActualData());
        reuse.setOgStatus(from.getOgStatus());
        reuse.setRuleApplied(from.getRuleApplied());
        reuse.setInfo(from.getInfo());
        reuse.setTags(from.getTags());
        return reuse;
    }

    @Override
    public int getLength() {
        return -1; // Variable length
    }

    @Override
    public void serialize(StatusMetric record, DataOutputView target) throws IOException {
        target.writeUTF(record.getGroup());
        target.writeUTF(record.getFunction());
        target.writeUTF(record.getService());
        target.writeUTF(record.getHostname());
        target.writeUTF(record.getMetric());
        target.writeUTF(record.getStatus());
        target.writeUTF(record.getTimestamp());
        target.writeInt(record.getDateInt());
        target.writeInt(record.getTimeInt());
        target.writeUTF(record.getSummary());
        target.writeUTF(record.getMessage());
        target.writeUTF(record.getPrevState());
        target.writeUTF(record.getPrevTs());
        target.writeUTF(record.getActualData());
        target.writeUTF(record.getOgStatus());
        target.writeUTF(record.getRuleApplied());
        target.writeUTF(record.getInfo());
        target.writeUTF(record.getTags());
    }

    @Override
    public StatusMetric deserialize(DataInputView source) throws IOException {
        return new StatusMetric(
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readInt(),
                source.readInt(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF(),
                source.readUTF());
    }

    @Override
    public StatusMetric deserialize(StatusMetric reuse, DataInputView source) throws IOException {
        reuse.setGroup(source.readUTF());
        reuse.setFunction(source.readUTF());
        reuse.setService(source.readUTF());
        reuse.setHostname(source.readUTF());
        reuse.setMetric(source.readUTF());
        reuse.setStatus(source.readUTF());
        reuse.setTimestamp(source.readUTF());
        reuse.setDateInt(source.readInt());
        reuse.setTimeInt(source.readInt());
        reuse.setSummary(source.readUTF());
        reuse.setMessage(source.readUTF());
        reuse.setPrevState(source.readUTF());
        reuse.setPrevTs(source.readUTF());
        reuse.setActualData(source.readUTF());
        reuse.setOgStatus(source.readUTF());
        reuse.setRuleApplied(source.readUTF());
        reuse.setInfo(source.readUTF());
        reuse.setTags(source.readUTF());
        reuse.setHasThr(source.readBoolean());
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeInt(source.readInt());
        target.writeInt(source.readInt());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeUTF(source.readUTF());
        target.writeBoolean(source.readBoolean());
    }

//    @Override
//    public TypeSerializerSnapshot<StatusMetric> snapshotConfiguration() {
//        return new StatusMetricSerializerSnapshot();
//    }

//    public static final class StatusMetricSerializerSnapshot extends SimpleTypeSerializerSnapshot<StatusMetric> {
//        public StatusMetricSerializerSnapshot() {
//            super(() -> INSTANCE);
//        }
//    }

    @Override
    public TypeSerializerSnapshot<StatusMetric> snapshotConfiguration() {
        return new StatusMetricSerializerSnapshot();
    }

    public static final class StatusMetricSerializerSnapshot extends SimpleTypeSerializerSnapshot<StatusMetric> {
        public StatusMetricSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}