_____________________________________________________________________________________________
_____________________________________________________________________________________________
				LWPTrends_v1811
				December 2018
_____________________________________________________________________________________________
_____________________________________________________________________________________________
This file includes functions that perform trend analysis and also that aggregate trend analysis outputs from many sites.
_____________________________________________________________________________________________
---- Trend Analysis Functions -------------

These functions are designed to undertake water quality trend analyses using the R statistical computing environment. 
The functions operate on data pertaining to a single site + variable.
The functions can be used to analyse data that pertains to many sites + variable combinations by applying the functions to appropriately sub-setted data using (for example the ddply function from the plyr package).

To get started put all the files in a folder and run the “RunLWPTrendsExample_Dec_18.R” file.
_____________________________________________________________________________________________
---- Trend Agregation Functions -------------
These trend aggregation functions are designed to implement methods for aggregating individual water quality site trends into summaries in tabular, graphical or map format as part of environmental reporting. The functions are written for use in the R statistical computing environment. 

The functions operate on output from the LWP-Trends data for multiple sites.
The functions can be used to analyse data that pertains to many sites + variable combinations by applying the functions to appropriately sub-setted data using (for example the ddply function from the plyr package).

To get started put all the files in a folder and run the “RunLWPTrendsExampleAGGREGATION_Dec_18.r” file.

For a complete explanation of the methods see Snelder and Fraser (2018).
Snelder, T. and C. Fraser, 2008. Aggregating Trend Data for Environmental Reporting. LWP Client Report 2018-01, LWP Ltd, Christchurch, New Zealand.

_____________________________________________________________________________________________
A complete explanation of how to use the functions is provided in the file "LWPTrendsHelp_2018_07_18.pdf".

_____________________________________________________________________________________________
_____________________________________________________________________________________________
Disclaimer
Software downloaded from the landwaterpeople (LWP) website (or provided by direct communication with an LWP employee) is provided 'as is' without warranty of any kind, either express or implied, including, but not limited to, the implied warranties of fitness for a purpose, or the warranty of non-infringement. Without limiting the foregoing, LWP makes no warranty that:
1.	the software will meet your requirements
2.	the software will be uninterrupted, timely, secure or error-free
3.	the results that may be obtained from the use of the software will be effective, accurate or reliable
4.	the quality of the software will meet your expectations
5.	any errors in the software obtained from the LWP web site will be corrected.
Software and its documentation made available on the LWP website:
1.	could include technical or other mistakes, inaccuracies, or typographical errors. LWP may make changes to the software or documentation made available on its website.
2.	may be out of date, and LWP makes no commitment to update such materials.
LWP assumes no responsibility for errors or omissions in the software or documentation available from its website.
In no event shall LWP be liable to you or any third parties for any special, punitive, incidental, indirect or consequential damages of any kind, or any damages whatsoever, including, without limitation, those resulting from loss of use, data or profits, whether or not LWP has been advised of the possibility of such damages, and on any theory of liability, arising out of or in connection with the use of this software.
The use of the software downloaded through the LWP site is done at your own discretion and risk and with agreement that you will be solely responsible for any damage to your computer system or loss of data that results from such activities. No advice or information, whether oral or written, obtained by you from LWP or from the LWP website shall create any warranty for the software.
