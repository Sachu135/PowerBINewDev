Readme for OneClickBatch File

Step1. Go to your C:/ drive create a folder 
       "kockpit-tools" and download the batch file and wget setup  from the following link
       Link : https://github.com/anmolpal/WindowsBatch.git
       The file will be downloaded in a zip file.Extract that zip file.
       Paste the extracted zip file into your kockpit-tools directory.

Step2. Double click on tha batch file and start the setup.

Step3. First it will welcome you with "Welcome to one click setup"
       Now, it will ask you if you want to install the wget setup. Press y and install that. 
       After installing wget in your system, the command prompt will exit.
       Double click on the batch file and start the setup again.
       This time do not install wget enter "n" in the prompt and move on to the next step
       which is Java Installation.
       Whenever a prompt asks you to Press any key to continue. Just press anykey on your
       keyboard and it will move on to the next section.

Step4. First it will look for Java in your system, if Java is not installed in your system 
       a JAVA 8 setup will up on your screen. Install that setup.
       Same goes for Python but when Installer pop up do remember to click on "add to path"
       while installing Python.
       
Step5. Next it will look for Spark in your "C:\ " directory. If spark is not installed it will get 
       downloaded automatically to your C drive. Same goes for eclipse.
       
Step6. Spark Paths will be automatically added into your environment variables.
       To check if environment variables are succesfully exported, open your windows search 
       bar and type "edit the system environment variables". Click on environment variables
       and you can view there that your spark path is succesfully exported or not.

Step7. At last, PostgreSQL will be installed. A prompt will ask you to whether you want to install
       PostgreSQL or not. Enter y if you want to install it (Do remember to enter PostgreSQL password 
              as "sa@123" while you were installing PostgreSQL).

              
  