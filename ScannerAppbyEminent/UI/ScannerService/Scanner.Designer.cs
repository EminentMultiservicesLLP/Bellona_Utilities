namespace ScannerService
{
    partial class Scanner
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnScan = new System.Windows.Forms.Button();
            this.progressTextBox = new System.Windows.Forms.RichTextBox();
            this.progressBar1 = new System.Windows.Forms.ProgressBar();
            this.backgroundPageScan = new System.ComponentModel.BackgroundWorker();
            this.btnSendDocument = new System.Windows.Forms.Button();
            this.openFileDialog1 = new System.Windows.Forms.OpenFileDialog();
            this.SuspendLayout();
            // 
            // btnScan
            // 
            this.btnScan.Location = new System.Drawing.Point(13, 12);
            this.btnScan.Name = "btnScan";
            this.btnScan.Size = new System.Drawing.Size(91, 54);
            this.btnScan.TabIndex = 0;
            this.btnScan.Text = "Start Scan";
            this.btnScan.UseVisualStyleBackColor = true;
            this.btnScan.Click += new System.EventHandler(this.btnScan_Click);
            // 
            // progressTextBox
            // 
            this.progressTextBox.Location = new System.Drawing.Point(13, 72);
            this.progressTextBox.Name = "progressTextBox";
            this.progressTextBox.Size = new System.Drawing.Size(587, 121);
            this.progressTextBox.TabIndex = 1;
            this.progressTextBox.Text = "";
            // 
            // progressBar1
            // 
            this.progressBar1.Location = new System.Drawing.Point(110, 42);
            this.progressBar1.Name = "progressBar1";
            this.progressBar1.Size = new System.Drawing.Size(490, 23);
            this.progressBar1.TabIndex = 3;
            // 
            // backgroundPageScan
            // 
            this.backgroundPageScan.DoWork += new System.ComponentModel.DoWorkEventHandler(this.backgroundPageScan_DoWork);
            // 
            // btnSendDocument
            // 
            this.btnSendDocument.Location = new System.Drawing.Point(110, 8);
            this.btnSendDocument.Name = "btnSendDocument";
            this.btnSendDocument.Size = new System.Drawing.Size(119, 28);
            this.btnSendDocument.TabIndex = 4;
            this.btnSendDocument.Text = "Test Send Document";
            this.btnSendDocument.UseVisualStyleBackColor = true;
            this.btnSendDocument.Visible = false;
            this.btnSendDocument.Click += new System.EventHandler(this.button1_Click);
            // 
            // openFileDialog1
            // 
            this.openFileDialog1.Filter = "\"pdf files (*.pdf)|*.pdf,*.PDF\"";
            this.openFileDialog1.Title = "Choose PDF file for Upload";
            // 
            // Scanner
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(604, 199);
            this.Controls.Add(this.btnSendDocument);
            this.Controls.Add(this.progressBar1);
            this.Controls.Add(this.progressTextBox);
            this.Controls.Add(this.btnScan);
            this.Name = "Scanner";
            this.Text = "Document Scanner";
            this.Load += new System.EventHandler(this.Scanner_Load);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnScan;
        private System.Windows.Forms.RichTextBox progressTextBox;
        private System.Windows.Forms.ProgressBar progressBar1;
        private System.ComponentModel.BackgroundWorker backgroundPageScan;
        private System.Windows.Forms.Button btnSendDocument;
        private System.Windows.Forms.OpenFileDialog openFileDialog1;
    }
}

