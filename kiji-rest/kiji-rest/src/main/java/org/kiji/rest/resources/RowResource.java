package org.kiji.rest.resources;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.kiji.rest.RoutesConstants;
import org.kiji.rest.rows.KijiRow;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.tools.ToolUtils;

import com.google.common.collect.Lists;

@Path(RoutesConstants.ROW_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class RowResource extends AbstractKijiResource {

  public RowResource(KijiURI pCluster, Set<KijiURI> pInstances) {
    super(pCluster, pInstances);
  }

  private long[] getTimestamps(String pTimeRange)
  {
    long[] lReturn = new long[] {0, Long.MAX_VALUE};
    final Pattern timestampPattern = Pattern.compile("([0-9]*)\\.\\.([0-9]*)");
    final Matcher timestampMatcher = timestampPattern.matcher(pTimeRange);
    
    if (timestampMatcher.matches()) {
      lReturn[0] = ("".equals(timestampMatcher.group(1))) ? 0
          : Long.parseLong(timestampMatcher.group(1));
      final String rightEndpoint = timestampMatcher.group(2);
      lReturn[1] = ("".equals(rightEndpoint)) ? Long.MAX_VALUE : Long.parseLong(rightEndpoint);
    }
    return lReturn;
  }

  
  private List<KijiColumnName> addColumnDefs( 
      KijiTableLayout pLayout,
      ColumnsDef pColumnsDef,
      String pRequestedColumns)
  {
    List<KijiColumnName> lReturnCols = Lists.newArrayList();
    Collection<KijiColumnName> lColumnsRequested = null;
    //Check for whether or not *all* columns were requested
    if(pRequestedColumns == null ||
        pRequestedColumns.trim().equals("*"))
    {
      lColumnsRequested = pLayout.getColumnNames();
    }
    else
    {
      lColumnsRequested = Lists.newArrayList();
      String[] pColumns = pRequestedColumns.split(",");
      for(String s:pColumns)
      {
        lColumnsRequested.add(new KijiColumnName(s));
      }
    }
    
    Map<String,FamilyLayout> colMap = pLayout.getFamilyMap();
    
    for(KijiColumnName kijiColumn:lColumnsRequested)
    {
      FamilyLayout layout = colMap.get(kijiColumn.getFamily());
      if(null != layout) {
        
        if(layout.isMapType())
        {
          pColumnsDef.add(kijiColumn);
          lReturnCols.add(kijiColumn);          
        }
        else
        {
          Map<String,ColumnLayout> groupColMap = layout.getColumnMap();
          if(kijiColumn.isFullyQualified())
          {
            ColumnLayout groupColLayout = groupColMap.get(kijiColumn.getQualifier());
            if(null != groupColLayout)
            {
              pColumnsDef.add(kijiColumn);
              lReturnCols.add(kijiColumn);
            }
            else
            {
              //Log that qualifier for given family doesn't exist?
            }
          }
          else
          {
            for(ColumnLayout c:groupColMap.values())
            {
              KijiColumnName fullyQualifiedGroupCol = new KijiColumnName(kijiColumn.getFamily(), c.getName());
              pColumnsDef.add(fullyQualifiedGroupCol);
              lReturnCols.add(fullyQualifiedGroupCol);              
            }
          }
        }
      }
      else
      {
        //Log that the column family requested doesn't exist?
      }
    }    
    return lReturnCols;
  }
  
  @GET
  public KijiRow getRow(@PathParam(RoutesConstants.INSTANCE_PARAMETER) String pInstanceId,
      @PathParam(RoutesConstants.TABLE_PARAMETER) String pTableId,
      @PathParam(RoutesConstants.HEX_ENTITY_ID_PARAMETER) String pHexEntityId,
      @QueryParam("cols") @DefaultValue("*") String pColumns,
      @QueryParam("versions") @DefaultValue("1") int pMaxVersions,
      @QueryParam("timerange") String pTimeRange) {
    
//      super.validateInstance(pInstanceId);
      KijiRow returnRow = null;
      
      try
      {
          final KijiTable table = super.getKijiTable(pInstanceId, pTableId);
          try {
            final KijiTableReader reader = table.openTableReader();
            try {
              // Select which columns you want to read:
              KijiDataRequestBuilder dataBuilder = KijiDataRequest.builder();
              //Build up the request
              if(pTimeRange != null)
              {
                long[] lTimeRange = getTimestamps(pTimeRange);
                dataBuilder.withTimeRange(lTimeRange[0], lTimeRange[1]);
              }
 
              ColumnsDef colsRequested = dataBuilder.newColumnsDef().withMaxVersions(pMaxVersions);
              List<KijiColumnName> lRequestedColumns = 
                  addColumnDefs(table.getLayout(), colsRequested, pColumns);
              //We can't process a data request with no columns.
              if(lRequestedColumns.isEmpty())
              {
                throw new WebApplicationException(Status.BAD_REQUEST);
              }
              
              final KijiDataRequest dataRequest = dataBuilder.build();
              final EntityId entityId = ToolUtils.createEntityIdFromUserInputs(pHexEntityId,
                  table.getLayout());
              final KijiRowData rowData = reader.get(entityId, dataRequest);
              // Use the row:
              // É
              returnRow = getKijiRow(rowData, table.getLayout(), lRequestedColumns);
            } finally {
              // Always close the reader you open:
              reader.close();
            }       
          } finally {
            // Always release the table you open:
            table.release();
          }
      }
      catch(IOException e)
      {
        throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
      }
      
      return returnRow;
  }
  
  private KijiRow getKijiRow(KijiRowData pRowData, KijiTableLayout pTableLayout, 
      List<KijiColumnName> pColumnsRequested)
  {
    KijiRow returnRow = new KijiRow(pRowData.getEntityId());
    Map<String,FamilyLayout> familyLayoutMap = pTableLayout.getFamilyMap();
    
    for(KijiColumnName col:pColumnsRequested)
    {
      FamilyLayout familyInfo = familyLayoutMap.get(col.getFamily());
      CellSpec spec;
      try {
        spec = pTableLayout.getCellSpec(col);
        if(spec.isCounter())
        {
          if(col.isFullyQualified())
          {
            KijiCell<Long> counter = pRowData.getMostRecentCell(col.getFamily(),
                col.getQualifier()); 
            if(null != counter)
            {
              returnRow.addCell(new org.kiji.rest.rows.KijiCell(col,counter.getTimestamp(),
                  counter.getData()));
            }
          }
          else if(familyInfo.isMapType()) //Only can print all qualifiers on map types
          {
            for (String key : pRowData.getQualifiers(col.getFamily())) {
              KijiCell<Long> counter = pRowData.getMostRecentCell(col.getFamily(), key);
              if (null != counter) {
                returnRow.addCell(new org.kiji.rest.rows.KijiCell(col,counter.getTimestamp(),
                    counter.getData()));
              }
            }
          }
        }
        else
        {
          if(col.isFullyQualified())
          {
            Map<Long,Object> rowVals = pRowData.getValues(col.getFamily(), col.getQualifier());
            for (Entry<Long, Object> timestampedCell:rowVals.entrySet()) {
              returnRow.addCell(new org.kiji.rest.rows.KijiCell(col,timestampedCell.getKey(),
                  timestampedCell.getValue()));
            }            
          }
          else if(familyInfo.isMapType())
          {
            NavigableMap<String, NavigableMap<Long, Object>> keyTimeseriesMap =
                pRowData.getValues(col.getFamily());
            for (String key : keyTimeseriesMap.keySet()) {
              for (Entry<Long, Object> timestampedCell : keyTimeseriesMap.get(key).entrySet()) {
                returnRow.addCell(new org.kiji.rest.rows.KijiCell(col,timestampedCell.getKey(),
                    timestampedCell.getValue()));
              }
            }            
          }
        }
      } catch (IOException e) {
        //All columns at this point have been validated so this shouldn't happen.
      }
    }
    return returnRow;
  }
}
