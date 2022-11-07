Readme For Bash Automation Standalone

[Most Important :: The user must be same on master and all slave servers (e.g if master has a user
                    padmin the slave also must have padmin user.)]

Optional : Download sublime text and open this in sublime. In the bottom right corner select Bash and 
           it just becomes easy for you to read this file.

Step 1. SSH to your linux server by using the following command "ssh username@IP" (e.g anmol@192.10.15.758)
        After that enter your username and password

Step 2. (for master) When you open the terminal you are in the home directory. When you open the terminal you are 
        in the home directory. Download windows terminal from microsoft store
        and go to the location in Windows where your file is stored and right click on that file and open it the
        terminal and use command "scp -rv /OneClickMasterMultinode.sh username@IP:/home/" (Secure Copy or SCP is a means
        of securley transferring files between two machines on a network.)


Step 3. (for master) Now type "bash OneClickMasterMultinode.sh"

Step 4. (for master) First, it will ask you for "Do you want to add master and slave IP's in your hosts file? [yes/no]"
        Enter the no of hosts you want to add and there IP.

Step 5. (for master) After that enter your username of your master server. To know your linux server
        username type the command in your terminal "whoami".

        Now, it will check for linux system update. Your linux server will be automatically updated.
        Enter y to update your linux system.
        Next it will create kockpit-tools directory in your system
        Next it will look for Java and Python in your server, if they are pre-installed the installation
        will be skipped else Java and Python will be downloaded in your master server.To download
        Java Enter y and Java will get downloaded. Enter the same in case of Python.  

Step 6. (for master) In the next step, Hadoop will be downloaded in your master server.
        The first prompt will ask for "Enter your username for change of ownership from root to user:".
        Enter your linux server username in it.

        The second prompt will ask for "Enter the number of servers you want to replicate data:"
        Enter the number of slave servers you want to replicate your data (e.g it can be 1,2 0r 100)

        The third prompt ask you for "Enter the number of master IP's you want to add in your 
        hadoop master file (e.g 1, 2 or 9):".
        Enter the number of master server and there IP.
        
        The fourth prompt ask you for "Enter the number of slave IP's you want to add in your 
        hadoop master file (e.g 1, 2 or 9):".
        Enter the number of slave server and there IP.

Step 7. (for master) Now, Spark will be installed in your system.

        First prompt will ask you for "Example for how to create worker cores and memory
        export SPARK_WORKER_CORES=32
        export SPARK_WORKER_MEMORY=60g"
        Enter the number of worker cores you want to allocate (e.g 6, 12, 24, 32.......)
        Enter the number of worker memory you want to allocate (e.g 60g, 120g, 240g, 320g.......)

        In the second prompt of the Spark installation it will ask you this
        "Enter the master cluster IP which you want to add in your spark slaves file : "
        Enter your master cluster IP.

        In the third prompt of the Spark installation it will ask you this
        Enter the number of slave IP's you want to add in your spark slaves file (e.g 1, 2 or 9) :
        Enter the number of slave servers and there IP's


Step 8. After the spark installation it will move on to the Configuring SSH section. 
        First prompt will ask you for "Do you want to add SSH key to the home directory? [yes/no]"
        Enter yes and this prompt will appear "Enter file in which to save the key (/home/anmol/.ssh/id_rsa):"
        Just press enter 3 times and skip the "file location and passphrase part"

        Second prompt ask you for "Enter the number of slave IP's you want to copy from 
        master cluster (e.g 1, 2 or 9) :"
        Enter the number of slave servers you want to copy from your master server SSH key to slave
        server SSH key.
        (e.g anmol@192.11.17.144)        


Step 9. Now it will move on to the "Adding Paths to .bashrc" section.
        First prompt will ask you to "Do you want to add PATH to .bashrc? [yes/no]"
        Enter yes and the paths are added in your bashrc file.

Step 10. Next prompt will ask you to install Delta Lake. 
         Enter yes if you want to install Delta Lake.
         The output will be like this:
         2021-07-12 08:20:22,270 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
         Setting default log level to "WARN".
         To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
         Welcome to
               ____              __
              / __/__  ___ _____/ /__
             _\ \/ _ \/ _ `/ __/  '_/
            /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
               /_/
         
         Using Python version 3.6.9 (default, Jan 26 2021 15:33:00)
         Spark context Web UI available at http://anmol1.internal.cloudapp.net:4040
         Spark context available as 'sc' (master = local[*], app id = local-1626078023852).
         SparkSession available as 'spark'.
         >>>'

         The above output will take you to the Spark shell.
         Type "exit()" in the spark shell and move on to the Airflow section.


Step 11. After adding paths to bashrc it will move on to the next section which is
         "Installing Airflow V2.1".
         Enter y to download Airflow.
         Airflow gets automatically downloaded. The output will be like this.
         * Debug mode: on
        [2021-07-11 09:19:04,196] {_internal.py:113} INFO -  * Running on http://0.0.0.0:8181/ (Press CTRL+C to quit)
        [2021-07-11 09:19:04,198] {_internal.py:113} INFO -  * Restarting with stat
          ____________       _____________
         ____    |__( )_________  __/__  /________      __
        ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
        ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
         _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
        Starting the web server on port 8181 and host 0.0.0.0.
        [2021-07-11 09:19:06,568] {_internal.py:113} WARNING -  * Debugger is active!
        [2021-07-11 09:19:06,568] {_internal.py:113} INFO -  * Debugger PIN: 154-232-389

        Open your browser and go to master IP:8181.(Check on your master server that port 8181 is open)
        Login to airflow using the username admin and the password is also admin.


Step 12.(on your slave server) Now open your slave servers using SSH as we have done in Step1 and use 
        your slave server IP and the username, password to login. 
        When you open the terminal you are in the home directory. When you open the terminal you are 
        in the home directory. Download windows terminal from microsoft store
        and go to the location in Windows where your file is stored and right click on that file and open it the
        terminal and use command "scp -rv /OneClickSlaveMultinode.sh username@IP:/home/" (Secure Copy or SCP is a means
        of securley transferring files between two machines on a network.)

         "bash OneClickSlaveMultinode.sh" and perform step number 4,5,6,7,9 as we have already done 
         in our master server.  
       
Step 13.(on master) Type the command "source ~/.bashrc".
         Now open a new terminal and do again "bash OneClickMasterMultinode.sh"
         and Skip the first prompt where it asks you to enter hosts, just enter no.
         Enter your username in the second prompt and after every prompt just enter no
         and come straight to the "Hadoop and Spark daemons"
         Enter yes and your hadoop and spark demons will start up.
         Now open your browser and go to IP:9870 for Hadoop UI
         and for Spark UI go to IP:8080



