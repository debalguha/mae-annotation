package edu.brandeis.cs.nlp.mae.database;

import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ARG_FCOL_ART;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ARG_FCOL_ETAG;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ARG_FCOL_LTAG;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ART_COL_NAME;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ART_FCOL_TT;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ATT_FCOL_AT;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ATT_FCOL_ETAG;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_AT_COL_NAME;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_AT_FCOL_TT;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_CI_COL_LOCATION;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_CI_FCOL_ETAG;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_ETAG_COL_TEXT;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_TAG_COL_TID;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_TAG_FCOL_TT;
import static edu.brandeis.cs.nlp.mae.model.DBSchema.TAB_TT_COL_NAME;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.stmt.UpdateBuilder;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;

import edu.brandeis.cs.nlp.mae.MaeException;
import edu.brandeis.cs.nlp.mae.io.AnnotationLoader;
import edu.brandeis.cs.nlp.mae.io.DTDLoader;
import edu.brandeis.cs.nlp.mae.io.MaeIODTDException;
import edu.brandeis.cs.nlp.mae.model.Argument;
import edu.brandeis.cs.nlp.mae.model.ArgumentType;
import edu.brandeis.cs.nlp.mae.model.Attribute;
import edu.brandeis.cs.nlp.mae.model.AttributeType;
import edu.brandeis.cs.nlp.mae.model.CharIndex;
import edu.brandeis.cs.nlp.mae.model.DBSchema;
import edu.brandeis.cs.nlp.mae.model.ExtentTag;
import edu.brandeis.cs.nlp.mae.model.LinkTag;
import edu.brandeis.cs.nlp.mae.model.MaeModelException;
import edu.brandeis.cs.nlp.mae.model.Tag;
import edu.brandeis.cs.nlp.mae.model.TagType;
import edu.brandeis.cs.nlp.mae.model.Task;
import edu.brandeis.cs.nlp.mae.util.FileHandler;
import edu.brandeis.cs.nlp.mae.util.MappedSet;
import edu.brandeis.cs.nlp.mae.util.SpanHandler;

public abstract class AbstractDriverImpl implements MaeDriverI{
	
	private static Logger logger = LoggerFactory.getLogger(AbstractDriverImpl.class);
	protected ConnectionSource cs;
	protected IdHandler idHandler;
    // this should be distinguishable over diff tasks and diff versions
	protected Task workingTask;
	protected boolean workChanged;

    protected Dao<Task, Integer> taskDao;
    protected Dao<CharIndex, Integer> charIndexDao;
    protected Dao<TagType, Integer> tagTypeDao;
    protected Dao<ExtentTag, String> eTagDao;
    protected Dao<LinkTag, String> lTagDao;
    protected Dao<AttributeType, Integer> attTypeDao;
    protected Dao<Attribute, Integer> attDao;
    protected Dao<ArgumentType, Integer> argTypeDao;
    protected Dao<Argument, Integer> argDao;

    protected QueryBuilder<CharIndex, Integer> charIndexQuery;
    protected QueryBuilder<TagType, Integer> tagTypeQuery;
    protected QueryBuilder<ExtentTag, String> eTagQuery;
    protected QueryBuilder<LinkTag, String> lTagQuery;
    protected QueryBuilder<AttributeType, Integer> attTypeQuery;
    protected QueryBuilder<Attribute, Integer> attQuery;
    protected QueryBuilder<ArgumentType, Integer> argTypeQuery;
    protected QueryBuilder<Argument, Integer> argQuery;

    protected Dao[] allDaos;
    protected QueryBuilder[] allQueryBuilders;
    
    @Override
    public void setupDatabase(ConnectionSource source) throws MaeDBException {

        try {
            taskDao = DaoManager.createDao(source, Task.class);
            charIndexDao = DaoManager.createDao(source, CharIndex.class);
            tagTypeDao = DaoManager.createDao(source, TagType.class);
            eTagDao = DaoManager.createDao(source, ExtentTag.class);
            lTagDao = DaoManager.createDao(source, LinkTag.class);
            attTypeDao = DaoManager.createDao(source, AttributeType.class);
            attDao = DaoManager.createDao(source, Attribute.class);
            argTypeDao = DaoManager.createDao(source, ArgumentType.class);
            argDao = DaoManager.createDao(source, Argument.class);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

        charIndexQuery = charIndexDao.queryBuilder();
        tagTypeQuery = tagTypeDao.queryBuilder();
        eTagQuery = eTagDao.queryBuilder();
        lTagQuery = lTagDao.queryBuilder();
        attTypeQuery = attTypeDao.queryBuilder();
        attQuery = attDao.queryBuilder();
        argTypeQuery = argTypeDao.queryBuilder();
        argQuery = argDao.queryBuilder();

        allDaos = new Dao[]{ taskDao, charIndexDao, tagTypeDao, eTagDao, lTagDao, attTypeDao, attDao, argTypeDao, argDao};
        allQueryBuilders = new QueryBuilder[]{ charIndexQuery, tagTypeQuery, eTagQuery, lTagQuery, attTypeQuery, attQuery, argTypeQuery, argQuery};

        dropAllTables(source);
        createAllTables(source);

    }

    public void createAllTables(ConnectionSource source) throws MaeDBException {
        for (Dao dao : allDaos) {
            try {
                TableUtils.createTable(source, dao.getDataClass());
            } catch (SQLException e) {
                throw catchSQLException(e);
            }

        }
    }

    public void dropAllTables(ConnectionSource source) throws MaeDBException {
        for (Dao dao : allDaos) {
            try {
                TableUtils.dropTable(source, dao.getDataClass(), true);
            } catch (SQLException e) {
                throw catchSQLException(e);
            }

        }
    }

    @Override
    public void readTask(File file) throws MaeIODTDException, MaeDBException, FileNotFoundException {
        DTDLoader dtdl = new DTDLoader(this);
        dropAllTables(cs);
        createAllTables(cs);
        if (!dtdl.read(file)) {
            throw new MaeIODTDException("DTD does not contain any definition, maybe not a DTD file? " + file.getAbsolutePath());
        }

    }

    @Override
    public String readAnnotation(File file) throws MaeException {
        AnnotationLoader xmll = new AnnotationLoader(this);
        String xmlParseWarnings =  xmll.loadFile(file);
        setAnnotationChanged(false);
        return xmlParseWarnings;

    }

    private void resetQueryBuilders() {
        for (QueryBuilder qb : allQueryBuilders) {
            qb.reset();
        }
    }

    @Override
    public String getAnnotationFileName() throws MaeDBException {
        return workingTask.getAnnotationFileName();
    }

    @Override
    public String getAnnotationFileBaseName() throws MaeDBException {
        return FileHandler.getFileBaseName(getAnnotationFileName());
    }

    @Override
    public void setAnnotationFileName(String fileName) throws MaeDBException {
        try {
            this.workingTask.setAnnotationFileName(fileName);
            taskDao.update(workingTask);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public String getPrimaryText() throws MaeDBException {
        return workingTask.getPrimaryText();
    }

    @Override
    public void setPrimaryText(String text) throws MaeDBException {
        try {
            this.workingTask.setPrimaryText(text);
            taskDao.update(workingTask);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public String getTaskName() {
        return workingTask.getName();
    }

    @Override
    public void setTaskName(String name) throws MaeDBException {
        try {
            // need to clear task table before updating id column of it
            TableUtils.clearTable(cs, taskDao.getDataClass());
            workingTask.setName(name);
            taskDao.create(workingTask);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public String getTaskFileName() throws MaeDBException {
        return workingTask.getTaskFileName();
    }

    @Override
    public void setTaskFileName(String fileName) throws MaeDBException {
        try {
            this.workingTask.setTaskFileName(fileName);
            taskDao.update(workingTask);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public boolean isTaskLoaded() {
        return workingTask.isTaskLoaded();
    }

    @Override
    public boolean isAnnotationLoaded() {
        return workingTask.isAnnotationLoaded();
    }

    @Override
    public void setAnnotationChanged(boolean b) {
        workChanged = b;
    }

    @Override
    public boolean isAnnotationChanged() {
        return isAnnotationLoaded() && workChanged;
    }

    @Override
    public boolean isPrimaryTextLoaded() {
        return workingTask.isPrimaryTextLoaded();
    }

    @Override
    public List<ExtentTag> getTagsAt(int location) throws MaeDBException {

        try {
            List<ExtentTag> results;
            charIndexQuery.where().eq(TAB_CI_COL_LOCATION, location);
            results = eTagQuery.join(charIndexQuery).query();
            resetQueryBuilders();
            return results;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<ExtentTag> getTagsIn(int[] locations) throws MaeDBException {
        Set<ExtentTag> tags = new TreeSet<>();
        for (int location : locations) {
            tags.addAll(getTagsAt(location));
        }
        return new ArrayList<>(tags);
    }

    public List<String> getTagIdsAt(int loc) throws MaeDBException {
        List<String> tids = new ArrayList<>();
        for (ExtentTag tag : getTagsAt(loc)) {
            tids.add(tag.getId());
        }
        return tids;
    }

    @Override
    public List<ExtentTag> getTagsOfTypeAt(TagType type, int location) throws MaeDBException {
        try {
            List<ExtentTag> results;
            charIndexQuery.where().eq(TAB_CI_COL_LOCATION, location);
            results = eTagQuery.join(charIndexQuery).where().eq(TAB_TAG_FCOL_TT, type).query();
            resetQueryBuilders();
            return results;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<ExtentTag> getTagsOfTypeIn(TagType type, int[] locations) throws MaeDBException {
        Set<ExtentTag> tags = new HashSet<>();
        for (int location : locations) {
            tags.addAll(getTagsOfTypeAt(type, location));
        }
        return new ArrayList<>(tags);
    }

    @Override
    public List<ExtentTag> getTagsOfTypeBetween(TagType type, int begin, int end) throws MaeDBException {
        Set<ExtentTag> tags = new HashSet<>();
        for (int i = begin; i < end; i++) {
            tags.addAll(getTagsOfTypeAt(type, i));
        }
        return new ArrayList<>(tags);
    }

    @Override
    public List<Integer> getAllAnchors() throws MaeDBException{
        List<Integer> anchors = new ArrayList<>();
        try {
            for (CharIndex location : charIndexDao.queryForAll()) {
                anchors.add(location.getLocation());
            }
            return anchors;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

    }

    @Override
    public List<Integer> getAllAnchorsOfTagType(TagType type) throws MaeDBException{

        try {
            List<CharIndex> locations;

            if (type.isExtent()) {
                eTagQuery.where().eq(TAB_TAG_FCOL_TT, type);
                locations = charIndexQuery.join(eTagQuery).query();

            } else {
                lTagQuery.where().eq(TAB_TAG_FCOL_TT, type);
                argQuery.join(lTagQuery).selectColumns(TAB_ARG_FCOL_ETAG).distinct();
                eTagQuery.join(argQuery);
                locations = charIndexQuery.join(eTagQuery).query();

            }

            ArrayList<Integer> locationList = new ArrayList<>();
            for (CharIndex ci : locations) {
                locationList.add(ci.getLocation());
            }
            resetQueryBuilders();
            return locationList;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

    }

    @Override
    public List<Integer> getAllAnchorsOfTagType(TagType type, List<TagType> exculdes) throws MaeDBException{
        List<Integer> targetSpans = getAllAnchorsOfTagType(type);
        for (TagType exclude : exculdes) {
            targetSpans.removeAll(getAllAnchorsOfTagType(exclude));
        }
        return targetSpans;

    }

    public List<ExtentTag> getArgumentTags(LinkTag linker) {
        return linker.getArgumentTags();
    }

    @Override
    public List<Integer> getAnchorsByTid(String tid) throws MaeDBException {
        Tag tag = getTagByTid(tid);
        if (tag.getTagtype().isExtent()) {
            return ((ExtentTag) tag).getSpansAsList();
        } else {

            Set<Integer> argSpans = new TreeSet<>();
            for (ExtentTag arg : ((LinkTag) tag).getArgumentTags()) {
                argSpans.addAll(arg.getSpansAsList());
            }
            return new ArrayList<>(argSpans);
        }
    }

    @Override
    public Tag getTagByTid(String tid) throws MaeDBException {
        try {
            if (eTagDao.queryForId(tid) != null) {
                return eTagDao.queryForId(tid);
            } else {
                return lTagDao.queryForId(tid);
            }
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public TagType getTagTypeByTid(String tid) throws MaeDBException {
        return getTagByTid(tid).getTagtype();

    }

    @Override
    public String getNextId(TagType type) {
        return idHandler.getNextID(type);
    }

    @Override
    public void deleteTag(Tag tag) throws MaeDBException {
        try {
            if (tag instanceof ExtentTag) {
                eTagDao.delete((ExtentTag) tag);
            } else {
                lTagDao.delete((LinkTag) tag);
            }
            logger.debug("a tag is deleted: " + tag.getId());
            setAnnotationChanged(true);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public Set<LinkTag> getLinksHasArgumentTag(ExtentTag argument) throws MaeDBException{
        try {
            TreeSet<LinkTag> links = new TreeSet<>();
            List<Argument> results;
            results = argQuery.where().eq(TAB_ARG_FCOL_ETAG, argument).query();
            for (Argument result : results) {
                links.add(result.getLinker());
            }
            resetQueryBuilders();
            return links;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<LinkTag> getAllLinkTagsOfAllTypes() throws MaeDBException {
        try {
            return new ArrayList<>(lTagDao.queryForAll());
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<ExtentTag> getAllExtentTagsOfAllTypes(boolean consumingOnly) throws MaeDBException {
        try {
            return new ArrayList<>(eTagDao.queryForAll());
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    public MappedSet<TagType, ExtentTag> getAllExtentTagsByTypes(boolean consumingOnly) throws MaeDBException {
        MappedSet<TagType, ExtentTag> tagsByTypes = new MappedSet<>();
        for (TagType type : getAllTagTypes()) {
            if (type.isExtent()) {
                tagsByTypes.putCollection(type, type.getExtentTagsAsList(consumingOnly));
            }
        }
        return tagsByTypes;

    }

    @Override
    public MappedSet<TagType,ExtentTag> getTagsByTypesAt(int location) throws MaeDBException{
        MappedSet<TagType, ExtentTag> tags = new MappedSet<>();
        for (ExtentTag tag : getTagsAt(location)) {
            tags.putItem(tag.getTagtype(), tag);
        }
        return tags;
    }

    @Override
    public MappedSet<TagType,ExtentTag> getTagsByTypesIn(int... locations) throws MaeDBException{
        MappedSet<TagType, ExtentTag> tags = new MappedSet<>();
        for (int location : locations) {
            tags.merge(getTagsByTypesAt(location));
        }
        return tags;
    }

    public MappedSet<TagType,ExtentTag> getTagsByTypesIn(String spansString) throws MaeDBException {
        return getTagsByTypesIn(SpanHandler.convertStringToPairs(spansString));
    }

    public MappedSet<TagType,ExtentTag> getTagsByTypesIn(ArrayList<int[]> spansPairs) throws MaeDBException {
        MappedSet<TagType, ExtentTag> tags = new MappedSet<>();
        for (int[] pair : spansPairs) {
            tags.merge(getTagsByTypesBetween(pair[0], pair[1]));
        }
        return tags;
    }

    @Override
    public MappedSet<TagType,ExtentTag> getTagsByTypesBetween(int begin, int end) throws MaeDBException{
        MappedSet<TagType, ExtentTag> tags = new MappedSet<>();
        for (int i=begin; i<end; i++) {
            tags.merge(getTagsByTypesAt(i));
        }
        return tags;
    }

    public List<? extends Tag> getAllTagsOfType(TagType type) throws MaeDBException {
        try {
            tagTypeDao.refresh(type);
            return new ArrayList<>(type.getTags());
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ExtentTag> getAllExtentTagsOfType(TagType type) throws MaeDBException {
        return (List<ExtentTag>) getAllTagsOfType(type);
    }

    @Override
    public List<ExtentTag> getAllNCTagsOfType(TagType type) throws MaeDBException, IllegalArgumentException {
        try {
            charIndexQuery.selectColumns(DBSchema.TAB_CI_FCOL_ETAG);
            List<ExtentTag> nctags = eTagQuery.where().eq(TAB_TAG_FCOL_TT, type).and().notIn(TAB_TAG_COL_TID, charIndexQuery).query();
            resetQueryBuilders();
            return nctags;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<LinkTag> getAllLinkTagsOfType(TagType type) throws MaeDBException {
        return (List<LinkTag>) getAllTagsOfType(type);

    }

    @Override
    public TagType createTagType(String typeName, String prefix, boolean isLink) throws MaeDBException {
        try {
            TagType type  = new TagType(typeName, prefix, isLink);
            tagTypeDao.create(type);
            logger.debug("a new tag type is created: " + typeName);
            setAnnotationChanged(true);
            return type;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public AttributeType createAttributeType(TagType tagType, String attTypeName) throws  MaeDBException {
        try {
            AttributeType attType = new AttributeType(tagType, attTypeName);
            attTypeDao.create(attType);
            logger.debug("a new attribute type is created: " + attTypeName);
            setAnnotationChanged(true);
            return attType;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public AttributeType getAttributeTypeOfTagTypeByName(TagType type, String name) throws MaeDBException {
        try {
            AttributeType result
                    = attTypeQuery.where().eq(TAB_AT_FCOL_TT, type).
                    and().eq(TAB_AT_COL_NAME, name).queryForFirst();
            resetQueryBuilders();
            return  result;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public ArgumentType getArgumentTypeOfTagTypeByName(TagType type, String name) throws MaeDBException {
        try {
            ArgumentType result
                    = argTypeQuery.where().eq(TAB_ART_FCOL_TT, type).
                    and().eq(TAB_ART_COL_NAME, name).queryForFirst();
            resetQueryBuilders();
            return  result;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<AttributeType> getAttributeTypesOfTagType(TagType type) throws MaeDBException {
        try {
            return new ArrayList<>(attTypeDao.queryForEq(TAB_AT_FCOL_TT, type));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public MappedSet<String, String> getTagTypesAndAttTypes() throws MaeDBException {
        MappedSet<String, String> tagsAndAtts = new MappedSet<>();
        for (TagType tagType : getAllTagTypes()) {
            // TODO: 2016-04-17 19:23:06EDT support link tags as well in the future
            if (tagType.isExtent()) {
                tagsAndAtts.putCollection(tagType.getName(), tagType.getAttributeTypesAsString());
            }
        }
        return tagsAndAtts;
    }

    @Override
    public Map<String, String> getAttributeMapOfTag(Tag tag) throws MaeDBException {
        return tag.getAttributesWithNames();

    }

    @Override
    public ArgumentType createArgumentType(TagType tagType, String argTypeName) throws MaeDBException {
        try {
            ArgumentType argType = new ArgumentType(tagType, argTypeName);
            argTypeDao.create(argType);
            logger.debug("a new argument type is created: " + argTypeName);
            setAnnotationChanged(true);
            return argType;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public ExtentTag createExtentTag(String tid, TagType tagType, String text, int... spans) throws MaeDBException {
        try {
            ExtentTag tag = new ExtentTag(tid, tagType, getAnnotationFileName());
            tag.setText(text);
            for (CharIndex ci: tag.setSpans(spans)) {
                charIndexDao.create(ci);
            }
            eTagDao.create(tag);
            eTagDao.update(tag); //only after update(), all properties are saved
            boolean added = idHandler.addId(tagType, tid);
            if (!added) {
                throw new MaeDBException("tag id is already in DB!: " + tid);
            }
            logger.debug("a new extent tag is created: " + tid);
            setAnnotationChanged(true);
            return tag;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public ExtentTag createExtentTag(TagType tagType, String text, int...spans) throws MaeDBException {
        String tid = idHandler.getNextID(tagType);
        return createExtentTag(tid, tagType, text, spans);
    }

    public LinkTag createLinkTag(String tid, TagType tagType) throws MaeDBException {
        try {
            LinkTag link = new LinkTag(tid, tagType, getAnnotationFileName());
            lTagDao.create(link);
            boolean added = idHandler.addId(tagType, tid);
            if (!added) {
                throw new MaeDBException("tag id is already in DB!: " + tid);
            }
            logger.debug("a new link tag is created: " + tid);
            setAnnotationChanged(true);
            return link;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public LinkTag createLinkTag(TagType tagtype) throws MaeDBException {
        return createLinkTag(idHandler.getNextID(tagtype), tagtype);
    }

    public LinkTag createLinkTag(String tid, TagType tagType, HashMap<ArgumentType, ExtentTag> arguments) throws MaeDBException {
        LinkTag link = createLinkTag(tid, tagType);
        for (ArgumentType argType : arguments.keySet()) {
            addArgument(link, argType, arguments.get(argType));
        }
        try {
            lTagDao.update(link);
            return link;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public void deleteAttribute(Tag tag, AttributeType attType) throws MaeDBException {
        updateAttribute(tag, attType, null);
    }

    @Override
    public Attribute updateAttribute(Tag tag, AttributeType attType, String attValue) throws MaeDBException {
        logger.debug(String.format("adding an attribute '%s: %s' to tag %s (%s)", attType.getName(), attValue, tag.getId(), tag.getTagTypeName()));
        try {
            Attribute oldAtt = attQuery.where().eq(TAB_ATT_FCOL_ETAG, tag).and().eq(TAB_ATT_FCOL_AT, attType).queryForFirst();
            if (oldAtt != null) {
                logger.debug(String.format("an old attribute \"%s\" is deleted from \"%s\"", oldAtt.toString(), tag.toString()));
                attDao.delete(oldAtt);
                setAnnotationChanged(true);
            }
            if (attValue != null && attValue.length() > 0) {
                return addAttribute(tag, attType, attValue);
            } else {
                logger.debug("no new value is provided. leaving the attribute deleted");
                setAnnotationChanged(true);
                return null;
            }
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

    }

    @Override
    public Attribute addAttribute(Tag tag, AttributeType attType, String attValue) throws MaeDBException {
        try {
            Attribute att = new Attribute(tag, attType, attValue);
            attDao.create(att);
            refreshTag(tag);
            resetQueryBuilders();
            logger.debug(String.format("an attribute \"%s\" is attached to \"%s\"", att.toString(), tag.toString()));
            setAnnotationChanged(true);
            return att;
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeModelException e) {
            throw new MaeDBException("failed to add an attribute: " + e.getMessage(), e);
        }
    }

    void refreshTag(Tag tag) throws SQLException {
        if (tag.getTagtype().isExtent()) {
            eTagDao.update((ExtentTag) tag);
        } else {
            lTagDao.update((LinkTag) tag);
        }
    }

    @Override
    public void batchCreateExtentTags(final Collection<ExtentTag> tags) throws MaeDBException {
        try {
            eTagDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (ExtentTag tag : tags) {
                        eTagDao.create(tag);
                        if (!idHandler.addId(tag.getTagtype(), tag.getId())) {
                            throw new MaeDBException("tag id is already in DB!: " + tag.getId());
                        }
                    }
                    return null;
                }
            });
            logger.debug(String.format("%d tags are inserted",tags.size()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeModelException e) {
            throw new MaeDBException("failed to add an attribute: " + e.getMessage(), e);
        } catch (MaeDBException e) {
            throw e;
        } catch (Exception ignored) {
        }

    }

    @Override
    public void batchCreateAnchors(final Collection<CharIndex> anchors) throws MaeDBException {
        try {
            charIndexDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (CharIndex anchor : anchors) {
                        charIndexDao.create(anchor);
                    }
                    return null;
                }
            });
            logger.debug(String.format("%d anchors are inserted",anchors.size()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeModelException e) {
            throw new MaeDBException("failed to add an attribute: " + e.getMessage(), e);
        } catch (MaeDBException e) {
            throw e;
        } catch (Exception ignored) {
        }

    }

    @Override
    public void batchCreateLinkTags(final Collection<LinkTag> tags) throws MaeDBException {
        try {
            lTagDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (LinkTag tag : tags) {
                        lTagDao.create(tag);
                        if (!idHandler.addId(tag.getTagtype(), tag.getId())) {
                            throw new MaeDBException("tag id is already in DB!: " + tag.getId());
                        }
                    }
                    return null;
                }
            });
            logger.debug(String.format("%d tags are inserted",tags.size()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeDBException e) {
            throw e;
        } catch (Exception ignored) {
        }

    }

    @Override
    public void batchCreateAttributes(final Collection<Attribute> atts) throws MaeDBException {
        try {
            attDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (Attribute att : atts) {
                        attDao.create(att);
                    }
                    return null;
                }
            });
            logger.debug(String.format("%d attributes are inserted", atts.size()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeDBException e) {
            throw e;
        } catch (Exception ignored) {
        }

    }

    @Override
    public void batchCreateArguments(final Collection<Argument> args) throws MaeDBException {
        try {
            argDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (Argument arg : args) {
                        argDao.create(arg);
                    }
                    return null;
                }
            });
            logger.debug(String.format("%d arguments are inserted", args.size()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeDBException e) {
            throw e;
        } catch (Exception ignored) {
        }

    }

    @Override
    public Set<Attribute> batchAddAttributes(Tag tag, final Map<AttributeType, String> attributes) throws MaeDBException {
        final Set<Attribute> toBeAdded = new HashSet<>();
        try {
            for (AttributeType attType : attributes.keySet()) {
                Attribute att = new Attribute(tag, attType, attributes.get(attType));
                toBeAdded.add(att);
            }
            attDao.callBatchTasks(new Callable<Void>() {
                public Void call() throws Exception {
                    for (Attribute attribute : toBeAdded) {
                        attDao.create(attribute);
                    }
                    return null;
                }
            });
            refreshTag(tag);
            resetQueryBuilders();
            logger.debug(String.format("attributes \"%s\" are attached to \"%s\"", toBeAdded.toString(), tag.toString()));
            setAnnotationChanged(true);
            return toBeAdded;
        } catch (SQLException e) {
            throw catchSQLException(e);
        } catch (MaeModelException e) {
            throw new MaeDBException("failed to add an attribute: " + e.getMessage(), e);
        } catch (Exception ignored) {
        }
        return null;

    }

    @Override
    public Argument addArgument(LinkTag linker, ArgumentType argType, ExtentTag argument) throws MaeDBException {
        try {
            logger.debug(String.format("adding an argument '%s: %s' to tag %s (%s)", argType.getName(), argument == null ? "null" : argument.getId(), linker.getId(), linker.getTagTypeName()));
            try {
                Argument arg = new Argument(linker, argType, argument);
                argDao.create(arg);
                lTagDao.update(linker);
                logger.debug(String.format("an argument \"%s\" is attached to \"%s\"", argument.toString(), linker.toString()));
                setAnnotationChanged(true);
                return arg;

            } catch (SQLException e) {
                throw catchSQLException(e);
            }
        } catch (NullPointerException ex) {
            throw new MaeDBException("no such a tag is in DB");
        }
    }

    @Override
    public Argument UpdateArgument(LinkTag linker, ArgumentType argType, ExtentTag argument) throws MaeDBException {
        try {
            logger.debug(String.format("adding an argument '%s: %s' to tag %s (%s)", argType.getName(), argument == null ? "null" : argument.getId(), linker.getId(), linker.getTagTypeName()));
            try {
                Argument oldArg = argQuery.where().eq(TAB_ARG_FCOL_LTAG, linker).
                        and().eq(TAB_ARG_FCOL_ART, argType).queryForFirst();
                if (oldArg != null) {
                    argDao.delete(oldArg);
                    setAnnotationChanged(true);
                }
                resetQueryBuilders();
                if (argument != null) {
                    return addArgument(linker, argType, argument);
                } else {
                    logger.debug("no new argument is provided. leaving the argument deleted");
                    return null;
                }

            } catch (SQLException e) {
                throw catchSQLException(e);
            }
        } catch (NullPointerException ex) {
            throw new MaeDBException("no such a tag is in DB");
        }
    }

    @Override
    public boolean updateTagSpans(ExtentTag tag, int[] spans) throws MaeDBException {
        try {
            List<CharIndex> olds = charIndexQuery.where().eq(TAB_CI_FCOL_ETAG, tag).query();
            charIndexDao.delete(olds);
            for (CharIndex anchor : tag.setSpans(spans)) {
                charIndexDao.create(anchor);
            }
            resetQueryBuilders();
            if (eTagDao.update(tag) == 1) {
                setAnnotationChanged(true);
                resetQueryBuilders();
                return true;
            }
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
        return false;

    }

    @Override
    public boolean updateTagText(ExtentTag tag, String text) throws MaeDBException {
        try {
            UpdateBuilder<ExtentTag, String> updateBuilder = eTagDao.updateBuilder();
            updateBuilder.where().eq(TAB_TAG_COL_TID, tag.getId());
            updateBuilder.updateColumnValue(TAB_ETAG_COL_TEXT,  text);
            if (updateBuilder.update() == 1) {
                setAnnotationChanged(true);
                eTagDao.refresh(tag);
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;

    }

    @Override
    public void emptyAnnotations() throws MaeDBException {
        try {
            for (ExtentTag tag : eTagDao.queryForAll()) {
                eTagDao.delete(tag);
            }
            for (LinkTag tag : lTagDao.queryForAll()) {
                lTagDao.delete(tag);
            }
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
        idHandler = new IdHandler();

    }

    public List<TagType> getTagTypes(boolean includeExtent, boolean includeLink) throws MaeDBException {
        try {
            ArrayList<TagType> types = new ArrayList<>();
            for (TagType type : tagTypeDao.queryForAll()) {
                if (type.isLink() && includeLink) {
                    types.add(type);
                } else if (type.isExtent() && includeExtent) {
                    types.add(type);
                }
            }
            return types;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<TagType> getAllTagTypes() throws MaeDBException {
        return getTagTypes(true, true);
    }

    @Override
    public List<TagType> getExtentTagTypes() throws MaeDBException {
        return getTagTypes(true, false);
    }

    @Override
    public List<TagType> getLinkTagTypes() throws MaeDBException {
        return getTagTypes(false, true);
    }

    public List<TagType> getNonConsumingTagTypes() throws MaeDBException {
        try {
            ArrayList<TagType> types = new ArrayList<>();
            for (TagType type : tagTypeDao.queryForAll()) {
                if (type.isNonConsuming()) {
                    types.add(type);
                }
            }
            return types;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    public boolean idExists(String tid) throws MaeDBException {
        try {
            return (eTagDao.queryForId(tid) != null || lTagDao.queryForId(tid) != null);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

    }

    @Override
    public TagType getTagTypeByName(String typeName) throws MaeDBException {
        try {
            return tagTypeDao.queryForEq(TAB_TT_COL_NAME, typeName).get(0);
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public List<ArgumentType> getArgumentTypesOfLinkTagType(TagType link) throws MaeDBException {
        try {
            return new ArrayList<>(argTypeDao.queryForEq(TAB_ART_FCOL_TT, link));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public boolean setTagTypePrefix(TagType tagType, String prefix) throws MaeDBException {
        try {
            tagType.setPrefix(prefix);
            boolean success = tagTypeDao.update(tagType) == 1;
            if (success) {
                logger.debug(String.format("assigned prefix \"%s\" to a tag type: %s", prefix, tagType.getName()));
            } else {
                logger.error(String.format("failed to assign prefix \"%s\" to a tag type: %s", prefix, tagType.getName()));
            }
            return success;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public boolean setTagTypeNonConsuming(TagType tagType, boolean b) throws MaeDBException {
        try {
            tagType.setNonConsuming(b);
            boolean success = tagTypeDao.update(tagType) == 1;
            if (success) {
                logger.debug(String.format("set a tag type \"%s\" to be: %s", tagType.getName(), b? "non-consuming": "only-consuming"));
            } else {
                logger.error(String.format("failed to set a tag type \"%s\" to be: %s", tagType.getName(), b? "non-consuming": "only-consuming"));
            }
            return success;
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public void setAttributeTypeValueSet(AttributeType attType, List<String> validValues) throws MaeDBException {
        attType.setValuesetFromList(validValues);
        try {
            attTypeDao.update(attType);
            logger.debug(String.format("assigned a valid value set \"%s\" to an attribute type: %s", validValues.toString(), attType.getName()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    public void setAttributeTypeValueSet(AttributeType attType, String...validValues) throws MaeDBException {
        setAttributeTypeValueSet(attType, Arrays.asList(validValues));
    }

    @Override
    public void setAttributeTypeDefaultValue(AttributeType attType, String defaultValue) throws MaeDBException {
        try {
            attType.setDefaultValue(defaultValue);
            attTypeDao.update(attType);
            logger.debug(String.format("assigned the default value \"%s\" to an attribute type: %s", defaultValue, attType.getName()));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public void setAttributeTypeIDRef(AttributeType attType, boolean b) throws MaeDBException {
        try {
            attType.setIdRef(b);
            attTypeDao.update(attType);
            logger.debug(String.format("set an attribute type \"%s\" to be: %s", attType.getName(), b? "idref": "free-text"));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    @Override
    public void setAttributeTypeRequired(AttributeType attType, boolean b) throws MaeDBException {
        try {
            attType.setRequired(true);
            attTypeDao.update(attType);
            logger.debug(String.format("set an attribute type \"%s\" to be: %s", attType.getName(), b? "required": "optional"));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }

    }

    @Override
    public void setArgumentTypeRequired(ArgumentType argType, boolean b) throws MaeDBException {
        try {
            argType.setRequired(true);
            argTypeDao.update(argType);
            logger.debug(String.format("set an argument type \"%s\" to be: %s", argType.getName(), b? "required": "optional"));
        } catch (SQLException e) {
            throw catchSQLException(e);
        }
    }

    protected MaeDBException catchSQLException(SQLException e) {
        String message = "caught sql error: " + e.getMessage();
        logger.error(message);
        return new MaeDBException(message, e);
    }
}
