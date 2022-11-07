config = {
        "SourceDBConnection":{"url":"192.10.15.16","port":"1433","userName":"KockpitTesting","password":"Tcpl@123","databaseName":"TeamDev","dbtable":"information_schema.tables"},
     #"SourceDBConnection":{"url":"192.10.15.191","port":"1433","userName":"Kockpit","password":"Tcpl@2050","databaseName":"TeamLive","dbtable":"information_schema.tables"},
    
    
    "DbEntities" : 
    [
     #     {"Name" : "Team Computers Pvt Ltd$", "Location" : "DB1E2", "DatabaseName" : "TEAMLIVE","ActiveInactive":"Active","Year":2019,"Month":4},
        {"Name" : "Team Computers Pvt Ltd$", "Location" : "DB1E1", "DatabaseName" : "TeamDev","ActiveInactive":"true","Year":2017,"Month":4},
    ],
    "Dimensions" : 
    [
        {"DBEntity":"DB1E1","Active Dimension":['CUSTOMER','BRANCH','TARGETPROD','OT BRANCH','SUBBU','SBU','PRODUCT','SALESPER','VENDOR','PROJECT']}
    ],
    "TablesToIngest": 
    [
            
#Sales           
        
         {"Table": "Sales Invoice Header","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["No_"],"Defaults":["No_","Ship-to City","Posting Date","Currency Factor","Payment Terms Code","Salesperson Code"],"CheckOn": "timestamp","Columns":["No_","Ship-to City","Posting Date","Currency Factor","Payment Terms Code","Salesperson Code"],"Modules":["Sales"]},
        # {"Table": "Sales Invoice Line","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Document No_","Line No_"],"Defaults":["Document No_","Line No_","Dimension Set ID","Type","No_","Gen_ Bus_ Posting Group","Posting Date","Quantity","Amount","Gen_ Prod_ Posting Group","Charges To Customer"],"CheckOn": "timestamp","Columns":["Document No_","Line No_","Dimension Set ID","Type","No_","Gen_ Bus_ Posting Group","Posting Date","Quantity","Amount","Gen_ Prod_ Posting Group","Charges To Customer"],"Modules":["Sales"]},
        # {"Table": "Sales Cr_Memo Line","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Document No_","Line No_"],"Defaults":["Dimension Set ID","Document No_","Line No_","No_","Type","Gen_ Bus_ Posting Group","Gen_ Prod_ Posting Group","Posting Date","Quantity","Amount"],"CheckOn": "timestamp","Columns":["Dimension Set ID","Document No_","Line No_","No_","Type","Gen_ Bus_ Posting Group","Gen_ Prod_ Posting Group","Posting Date","Quantity","Amount"], "Modules":["Sales"]},
        # {"Table": "Sales Cr_Memo Header","TableType": "Transaction","TransactionColumn":"Posting Date","Defaults":["No_","Ship-to City","Posting Date","Currency Factor","Salesperson Code"],"Key": ["No_"],"CheckOn": "timestamp","Columns":["No_","Ship-to City","Posting Date","Currency Factor","Salesperson Code"], "Modules":["Sales"]},
        # {"Table": "Sales Header","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Document Type","No_"], "Defaults":["Document Type","No_","Shipment Date","Posting Date","Currency Factor","Promised Delivery Date","Cust_ Order Rec_ Date"],"CheckOn": "timestamp","Columns":["Document Type","No_","Shipment Date","Posting Date","Currency Factor","Promised Delivery Date","Cust_ Order Rec_ Date"],"Modules":["Sales"]},
        # {"Table": "G_L Entry","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Entry No_"],"Defaults":["Amount","Source Code","Dimension Set ID","G_L Account No_","Posting Date","Document No_"], "CheckOn": "timestamp","Columns":["Amount","Source Code","Dimension Set ID","G_L Account No_","Posting Date","Document No_"], "Modules":["Finance","Sales"]}, 
        # {"Table": "Detailed Cust_ Ledg_ Entry","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Entry No_"], "Defaults":["Cust_ Ledger Entry No_","Document No_","Entry Type","Amount","Amount (LCY)","Debit Amount","Posting Date "],"CheckOn": "timestamp","Columns":["Cust_ Ledger Entry No_","Document No_","Entry Type","Amount","Amount (LCY)","Debit Amount","Posting Date"], "Modules":["Finance","Sales"]},
        # {"Table": "Collection Details","TableType": "Transaction","TransactionColumn":"Invoice Date","Key": ["Collection ID"], "Defaults":["Collection ID","Customer No_","Customer Name","Delivery Date","Installation Date","Invoice No_","Invoice Date","NOD","Description","Original Amount","Collection Date","Remarks By Collection Team","SalesPerson Code","Documentation Due Date","Expected Collection Date","Expected Collection Date 2","Expected Collection Date 3","Expected Collection Date 4"] ,"CheckOn": "timestamp","Columns":["Collection ID","Customer No_","Customer Name","Delivery Date","Installation Date","Invoice No_","Invoice Date","NOD","Description","Original Amount","Collection Date","Remarks By Collection Team","SalesPerson Code","Documentation Due Date","Expected Collection Date","Expected Collection Date 2","Expected Collection Date 3","Expected Collection Date 4"],"Modules":["Finance","Sales","SCM"]},
        # {"Table": "Value Entry","TableType": "Transaction","TransactionColumn":"Posting Date","Key": ["Entry No_"],"Defaults":["Dimension Set ID","Item No_","Posting Date",'Invoiced Quantity','Entry Type','Source Code',"Document No_","Cost Amount Expected","Cost Amount (Actual)", "Cost Amount (Expected)","Item Ledger Entry Type","Document Line No_"], "CheckOn": "timestamp","Columns":["Dimension Set ID","Item No_","Posting Date",'Invoiced Quantity','Entry Type','Source Code',"Document No_","Cost Amount Expected","Cost Amount (Actual)", "Cost Amount (Expected)","Item Ledger Entry Type","Document Line No_"],"Modules":["Inventory","Sales","Purchase"]},
# #         
#Master
        # {"Table": "G_L Budget Entry","TableType": "Master","TransactionColumn":"","Key": ["Entry No_","Dimension Set ID"],"Defaults":["Entry No_","Budget Dimension 1 Code",'Budget Dimension 2 Code',"Budget Dimension 3 Code","Dimension Set ID","Budget Name","Description","G_L Account No_","Date","Amount"] , "CheckOn": "timestamp","Columns":["Entry No_","Budget Dimension 1 Code",'Budget Dimension 2 Code',"Budget Dimension 3 Code","Dimension Set ID","Budget Name","Description","G_L Account No_","Date","Amount"], "Modules":["Finance"]},       
        # {"Table": "Salesperson_Purchaser","TableType": "Master","TransactionColumn":"","Key": ["Code"],"Defaults":["Code","Name","E-Mail","Phone No_","RSM","BU Head"],"CheckOn": "timestamp","Columns":["Code","Name","E-Mail","Phone No_","RSM","BU Head"]},  
        # {"Table": "Location","TableType": "Master","TransactionColumn":"","Key": ["Code"],"Defaults":["Code","Name","Post Code","Country_Region Code","Use As In-Transit","State Code","Trading Location","City","Location type"], "CheckOn": "timestamp","Columns":["Code","Name", "Post Code","Country_Region Code","Use As In-Transit","State Code","Trading Location","City","Location type"], "Modules":["Inventory"]},
        # {"Table": "Customer","TableType": "Master","TransactionColumn":"","Key": ["No_"],"Defaults":["No_","Phone No_","Name","City","Chain Name"],"CheckOn": "timestamp","Columns":["No_","Phone No_","Name","City","Chain Name"]},
        # {"Table": "General Posting Setup","TableType": "Master","TransactionColumn":"","Key": ["Gen_ Bus_ Posting Group", "Gen_ Prod_ Posting Group"], "Defaults" : ["Gen_ Bus_ Posting Group" , "Gen_ Prod_ Posting Group", "Sales Account","COGS Account","Sales Credit Memo Account"], "CheckOn": "timestamp", "Columns" : ["Gen_ Bus_ Posting Group" , "Gen_ Prod_ Posting Group", "Sales Account","COGS Account","Sales Credit Memo Account"], "Modules":["Purchase","Sales"]},           
        # {"Table": "Sales Line","TableType": "Master","TransactionColumn":"","Key": ["Document Type", "Document No_","Line No_"],"Defaults":["Document No_","Quantity","Dimension Set ID","No_","Outstanding Quantity","Amount","Line Amount","DBName","EntityName"] ,"CheckOn": "timestamp","Columns":["Document No_","Quantity","Dimension Set ID","No_","Outstanding Quantity","Amount","Line Amount"],"Modules":["Sales"]},
        # {"Table": "Posted Str Order Line Details","TableType": "Master","TransactionColumn":"", "Key": ["Type", "Calculation Order", "Document Type", "Invoice No_", "Item No_", "Line No_", "Tax_Charge Type", "Tax_Charge Group", "Tax_Charge Code"],"Defaults":["Invoice No_","Line No_","Amount","Account No_","Type","Document Type","Tax_Charge Type"], "CheckOn": "timestamp","Columns":["Invoice No_","Line No_","Amount","Account No_","Type","Document Type","Tax_Charge Type"], "Modules":["Purchase","Sales"]},
        # {"Table": "Product Group","TableType": "Master","TransactionColumn":"","Key": ["Code"], "Defaults":["Code","Item Category Code","Description"],"CheckOn": "timestamp","Columns":["Code","Item Category Code","Description"]},
        # {"Table": "Item Category","TableType": "Master","TransactionColumn":"","Key": ["Code"], "Defaults":["Code","Description"],"CheckOn": "timestamp","Columns":["Code","Description"]},
        # {"Table": "G_L Account","TableType": "Master","TransactionColumn":"","Key": ["No_"],"Defaults":["No_","Name","Account Type","Income_Balance","Indentation","Totaling"] ,"CheckOn": "timestamp","Columns":["No_","Name","Account Type","Income_Balance","Indentation","Totaling"]},
        # {"Table": "Vendor","TableType": "Master","TransactionColumn":"","Key": ["No_"],"Defaults":["No_","Name","Name 2","Vendor Posting Group","Payment Terms Code","Post Code","State Code","City"], "CheckOn": "timestamp","Columns":["No_","Name","Name 2","Vendor Posting Group","Payment Terms Code","Post Code","State Code","City"]},
         {"Table": "Item","TableType": "Master","TransactionColumn":"","Key": ["No_"],"Defaults":["No_","Manufacturer Code","Description","Item Category Code","Base Unit of Measure","Product Group Code"],"CheckOn": "timestamp","Columns": ["No_","Manufacturer Code","Description","Item Category Code","Base Unit of Measure","Product Group Code"]},
        # {"Table": "Customer","TableType": "Master","TransactionColumn":"","Key": ["No_"],"Defaults":["No_","Name","City","Chain Name","Phone No_"],"CheckOn": "timestamp","Columns":["No_","Name","City","Chain Name","Phone No_"]},
        # {"Table": "Dimension Set Entry","TableType": "Master","TransactionColumn":"","Key": ["Dimension Set ID","Dimension Code"],"Defaults":["Dimension Set ID","Dimension Value Code","Dimension Code"],"CheckOn": "timestamp","Columns":["Dimension Set ID","Dimension Value Code","Dimension Code"]},
        # {"Table": "Dimension Value","TableType": "Master","TransactionColumn":"","Key": ["Dimension Code","Code"],"Defaults":["Dimension Code","Code","Name","Blocked","Consolidation Code","Indentation","Totaling","REGION"],"CheckOn": "timestamp","Columns":["Dimension Code","Code","Name","Blocked","Consolidation Code","Indentation","Totaling","REGION"]},
          
       
    ],

    "TablesToRename": 
    [
    {
      "Table": "Customer","Columns": {"oldColumnName": ["No_","Name","City","ChainName","Country_RegionCode","PostCode","StateCode"],"newColumnName": ["Link Customer","Customer Name","Customer City","Customer Group Name","Country Region Code","Customer Post Code","Customer State Code"]}},
      {"Table": "Salesperson_purchaser","Columns": {"oldColumnName": ["Code","Name"],"newColumnName": ["Link SalesPerson","Sales Person"]}},
      {"Table": "Item","Columns": {"oldColumnName": ["No_","Description","ManufacturerCode","Base Unit of Measure",],"newColumnName": ["Link Item","Item Description","Item Manufacturer","Unit"]}},
      {"Table": "Location","Columns": {"oldColumnName": ["Code","Name","City","PostCode","StateCode","Country_RegionCode"],"newColumnName": ["Link Location","Location Name","Location City","Location Post Code","Location State Code","Location Country Region Code"]}},
      {"Table": "Vendor","Columns": {"oldColumnName": ["No_","Name2","City","Vendor Posting Group", "Post Code", "StateCode"],"newColumnName": ["Link Vendor","Vendor Name","Vendor City","Vendor Posting Group New","Vendor Post Code","Vendor State Code"]}

    
    
    
    }
    ]
}

